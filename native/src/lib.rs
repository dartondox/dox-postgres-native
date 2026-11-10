//! Native Postgres driver for Dart (dox_postgres_native).
//!
//! All Postgres work runs on the thread of the Dart isolate. There is no
//! runtime thread and no async runtime. Each isolate has one link (see
//! `link.rs`): one mio `Poll` and the connections this isolate uses.
//! - `dpg_submit` (leaf call) takes a batch of request records from Dart.
//!   It writes each query to a connection at once.
//! - `dpg_run` (leaf call) reads the sockets that have data. It writes the
//!   result records of the finished queries into Dart's buffer. It never
//!   waits.
//! - A small idle helper thread per link watches the sockets while Dart is
//!   idle. It posts `[1]` to the Dart port when there is something to read.
//!   It also checks that the isolate is alive. Only the helper calls the
//!   Dart API: `dpg_submit` and `dpg_run` are leaf calls, which must not.
//!
//! One connection (see `conn.rs`) speaks the Postgres protocol by itself:
//! connect, TLS (rustls), login, and queries. Plain queries are pipelined:
//! many queries go out on one connection without waiting for the answers.
//! Results stay in the Postgres binary format. Dart decodes them.
//!
//! One pool per config and size, for the whole process: isolates that open
//! the same settings share it. Each open needs one `dpg_close`. The pool
//! counts the open connections of all isolates, so `max_size` is a limit
//! for the whole process. A connection belongs to one link at a time. A
//! link that has idle connections gives them to the pool when another
//! link waits (see `link.rs`).
//!
//! Request record (little endian, 32 byte head): u64 id, u64 pool,
//! u64 tx (0: no transaction), u8 kind (0 query, 1 begin, 2 commit,
//! 3 rollback), u8 reserved, u16 param count, u32 sql length. Then the sql,
//! then per param: u32 type oid (0: let the server pick), u8 format
//! (0 text, 1 binary), i32 length (-1: NULL), bytes.
//!
//! Result record (13 byte head): u64 id, u8 status, u32 body length.
//! - status 0 (rows): u16 column count, per column u32 type oid, u16 name
//!   length, name. Then u32 row count, per value i32 length (-1: NULL), bytes.
//! - status 1 (server error): u8 field count, per field u8 code (the
//!   Postgres error field code, eg. b'C'), u32 length, text.
//! - status 2 (other error): the message.
//! - status 3 (transaction started): u64 transaction id.
//!
//! TLS is rustls. `sslmode` in the config turns it on (`prefer` or
//! `require`); `dpg_connect` says how the certificate is checked, and which
//! CA file to trust.
use std::collections::{HashMap, VecDeque};
use std::ffi::{CStr, c_char, c_int, c_void};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;

mod conn;
mod link;

use conn::Conn;

/// Bump when the FFI functions or the record layout change.
const ABI_VERSION: i32 = 6;
const REQ_HEAD: usize = 32;
const RES_HEAD: usize = 13;
/// Statements kept per connection. Over this, the one used longest ago is
/// closed (LRU).
const CACHE_MAX: usize = 512;
/// Longest wait for one request (a free connection, connect and query),
/// like the default `queryTimeout` of the `postgres` package. Over it the
/// request fails with a clear error, so Dart never waits forever. A
/// transaction with no request for this long is rolled back.
/// ponytail: fixed. Make it a pool option when an app needs another value.
const TIMEOUT: Duration = Duration::from_secs(300);
const TIMED_OUT: &str = "dox_postgres_native: timeout, no answer in 300 s";

// ---- Dart API DL -------------------------------------------------------
// `NativeApi.initializeApiDLData` points to this table. We only need
// `Dart_PostCObject`, so we read it here instead of compiling dart_api_dl.c.

#[repr(C)]
struct ApiEntry {
    name: *const c_char,
    function: *const c_void,
}

#[repr(C)]
struct DartApi {
    major: c_int,
    minor: c_int,
    functions: *const ApiEntry,
}

/// `Dart_CObject` for a `kTypedData` (Uint8) value. 64-bit layout.
#[repr(C)]
struct DartCObject {
    ty: i32,
    _pad: i32,
    td_type: i32,
    _pad2: i32,
    length: isize,
    values: *const u8,
}

type PostFn = unsafe extern "C" fn(i64, *mut DartCObject) -> bool;
static POST: OnceLock<PostFn> = OnceLock::new();

/// Posts a few bytes to a Dart port (a copy). False when the port is
/// closed: the isolate is gone.
fn post(port: i64, data: &[u8]) -> bool {
    let Some(f) = POST.get() else { return false };
    let mut obj = DartCObject {
        ty: 7, // Dart_CObject_kTypedData
        _pad: 0,
        td_type: 2, // Dart_TypedData_kUint8
        _pad2: 0,
        length: data.len() as isize,
        values: data.as_ptr(),
    };
    unsafe { f(port, &mut obj) }
}

// ---- result records ----------------------------------------------------

/// The id of a request that Rust makes itself (eg. the rollback of a
/// transaction that timed out). It gets no result record.
const INTERNAL: u64 = u64::MAX;

/// A failed request: the status of its result record (1 server error, 2
/// other error) and the body.
#[derive(Clone, Debug)]
struct Fail(u8, Vec<u8>);

impl Fail {
    fn text(text: &str) -> Fail {
        Fail(2, text.as_bytes().to_vec())
    }
}

/// Starts a record. Gives back where it starts, for [done].
fn head(out: &mut Vec<u8>, id: u64, status: u8) -> usize {
    let at = out.len();
    out.extend_from_slice(&id.to_le_bytes());
    out.push(status);
    out.extend_from_slice(&[0; 4]);
    at
}

/// Writes the body length into the head of the record at `at`.
fn done(out: &mut [u8], at: usize) {
    let len = (out.len() - at - RES_HEAD) as u32;
    out[at + 9..at + 13].copy_from_slice(&len.to_le_bytes());
}

/// Adds a result record with this status and body.
fn record(out: &mut Vec<u8>, id: u64, status: u8, body: &[u8]) {
    if id == INTERNAL {
        return;
    }
    let at = head(out, id, status);
    out.extend_from_slice(body);
    done(out, at);
}

fn fail(out: &mut Vec<u8>, id: u64, f: &Fail) {
    record(out, id, f.0, &f.1);
}

// ---- request records ---------------------------------------------------

fn rd16(b: &[u8], p: usize) -> usize {
    u16::from_le_bytes([b[p], b[p + 1]]) as usize
}

fn rd32(b: &[u8], p: usize) -> u32 {
    u32::from_le_bytes(b[p..p + 4].try_into().unwrap())
}

fn rd64(b: &[u8], p: usize) -> u64 {
    u64::from_le_bytes(b[p..p + 8].try_into().unwrap())
}

/// A parameter, already encoded by Dart. `data` is a slice of the batch.
struct Param {
    oid: u32,
    binary: bool,
    data: Option<Bytes>,
}

/// One request record. `sql` and the params are slices of the batch.
struct Request {
    id: u64,
    pool: u64,
    tx: u64,
    kind: u8,
    sql: Bytes,
    params: Vec<Param>,
}

/// None when the record is broken.
fn parse(b: &Bytes, p: &mut usize) -> Option<Request> {
    let at = *p;
    if at + REQ_HEAD > b.len() {
        return None;
    }
    let count = rd16(b, at + 26);
    let sql_len = rd32(b, at + 28) as usize;
    let mut q = at + REQ_HEAD;
    if q + sql_len > b.len() {
        return None;
    }
    let sql = b.slice(q..q + sql_len);
    q += sql_len;
    let mut params = Vec::with_capacity(count);
    for _ in 0..count {
        if q + 9 > b.len() {
            return None;
        }
        let oid = rd32(b, q);
        let binary = b[q + 4] == 1;
        let len = rd32(b, q + 5) as i32;
        q += 9;
        let data = if len < 0 {
            None
        } else {
            let end = q + len as usize;
            if end > b.len() {
                return None;
            }
            let d = b.slice(q..end);
            q = end;
            Some(d)
        };
        params.push(Param { oid, binary, data });
    }
    *p = q;
    Some(Request {
        id: rd64(b, at),
        pool: rd64(b, at + 8),
        tx: rd64(b, at + 16),
        kind: b[at + 24],
        sql,
        params,
    })
}

// ---- config ------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
enum Ssl {
    Disable,
    Prefer,
    Require,
}

/// The connection settings, from a libpq style `key=value` config.
#[derive(Debug)]
struct Config {
    host: String,
    port: u16,
    user: String,
    password: Vec<u8>,
    dbname: Option<String>,
    options: Option<String>,
    application_name: Option<String>,
    connect_timeout: Option<Duration>,
    ssl: Ssl,
}

/// Reads `key=value` pairs, like libpq. A value may be in single quotes;
/// `\` escapes the next character. None for an unknown key or a bad value.
fn parse_config(s: &str) -> Option<Config> {
    let mut c = Config {
        host: "localhost".into(),
        port: 5432,
        user: String::new(),
        password: Vec::new(),
        dbname: None,
        options: None,
        application_name: None,
        connect_timeout: None,
        ssl: Ssl::Prefer,
    };
    let mut it = s.chars().peekable();
    loop {
        while it.next_if(|c| c.is_whitespace()).is_some() {}
        if it.peek().is_none() {
            return Some(c);
        }
        let mut key = String::new();
        while let Some(ch) = it.next_if(|&c| c != '=' && !c.is_whitespace()) {
            key.push(ch);
        }
        while it.next_if(|c| c.is_whitespace()).is_some() {}
        it.next_if_eq(&'=')?;
        while it.next_if(|c| c.is_whitespace()).is_some() {}
        let mut value = String::new();
        let quoted = it.next_if_eq(&'\'').is_some();
        loop {
            match it.next() {
                None if quoted => return None,
                None => break,
                Some('\'') if quoted => break,
                Some(ch) if !quoted && ch.is_whitespace() => break,
                Some('\\') => value.push(it.next()?),
                Some(ch) => value.push(ch),
            }
        }
        match key.as_str() {
            "host" => c.host = value,
            "port" => c.port = value.parse().ok()?,
            "user" => c.user = value,
            "password" => c.password = value.into_bytes(),
            "dbname" => c.dbname = Some(value),
            "options" => c.options = Some(value),
            "application_name" => c.application_name = Some(value),
            "connect_timeout" => {
                let secs: u64 = value.parse().ok()?;
                c.connect_timeout = (secs > 0).then(|| Duration::from_secs(secs));
            }
            "sslmode" => {
                c.ssl = match value.as_str() {
                    "disable" => Ssl::Disable,
                    "prefer" => Ssl::Prefer,
                    "require" => Ssl::Require,
                    _ => return None,
                }
            }
            _ => return None,
        }
    }
}

/// Where a connection goes.
#[derive(Clone, Debug)]
enum Addr {
    Tcp(SocketAddr),
    /// the socket file of a Unix socket host
    #[cfg(unix)]
    Unix(std::path::PathBuf),
}

/// The addresses of the host. A host that starts with `/` is a Unix
/// socket folder, like libpq: the socket is `<host>/.s.PGSQL.<port>`.
fn resolve(c: &Config) -> Result<Vec<Addr>, Fail> {
    if c.host.starts_with('/') {
        #[cfg(unix)]
        return Ok(vec![Addr::Unix(
            format!("{}/.s.PGSQL.{}", c.host, c.port).into(),
        )]);
        #[cfg(not(unix))]
        return Err(Fail::text(
            "dox_postgres_native: Unix sockets do not work on this platform",
        ));
    }
    let addrs: Vec<Addr> = (c.host.as_str(), c.port)
        .to_socket_addrs()
        .map_err(|e| dial_error(c, e))?
        .map(Addr::Tcp)
        .collect();
    if addrs.is_empty() {
        return Err(dial_error(c, "no address"));
    }
    Ok(addrs)
}

/// A connect error that says the host and port, so a wrong one is easy to
/// see. Never the password.
fn dial_error(c: &Config, e: impl std::fmt::Display) -> Fail {
    at_server(c, "error connecting to server", e)
}

/// A failed TLS handshake, with the host and port like [dial_error].
fn tls_error(c: &Config, e: impl std::fmt::Display) -> Fail {
    at_server(c, "error performing TLS handshake with server", e)
}

fn at_server(c: &Config, what: &str, e: impl std::fmt::Display) -> Fail {
    Fail::text(&format!("{what} at \"{}\", port {}: {e}", c.host, c.port))
}

// ---- pools -------------------------------------------------------------

/// Looks up the host again on a thread, so the isolate never waits for
/// DNS. Runs after a connect fails: the address may have changed (eg. a
/// database failover moves the name to another IP).
fn refresh(pool: &Arc<Pool>) {
    {
        let mut st = pool.state.lock().unwrap();
        if st.resolving {
            return;
        }
        st.resolving = true;
    }
    let p = pool.clone();
    std::thread::spawn(move || {
        let addrs = resolve(&p.cfg);
        let mut st = p.state.lock().unwrap();
        // a failed lookup keeps the old addresses, if there are any
        if addrs.is_ok() || st.addrs.is_err() {
            st.addrs = addrs;
        }
        st.resolving = false;
    });
}

/// A pool: the settings, and the count of its connections in all
/// isolates. Isolates that open the same settings share one.
struct Pool {
    /// the size and the config: the same key gives the same pool
    key: String,
    /// how many opens use this pool. The last close closes it.
    refs: AtomicUsize,
    max: usize,
    cfg: Config,
    /// None: no TLS (`sslmode=disable`)
    tls: Option<Arc<rustls::ClientConfig>>,
    closed: AtomicBool,
    /// the count of links in `state.waiting`, read without the lock
    wanted: AtomicUsize,
    /// the request timeout in ms (tests make it shorter)
    timeout_ms: AtomicU64,
    /// goes up after each CREATE, ALTER or DROP on a connection of this
    /// pool. A statement prepared before it is not used again: its param
    /// types were picked for the old tables.
    schema: AtomicU64,
    state: Mutex<PoolState>,
}

struct PoolState {
    /// connections that exist now, in all links and in `idle`
    open: usize,
    /// connections no link uses. They are not in any poll.
    idle: Vec<Conn>,
    /// links that wait for a connection, first come first: (link id,
    /// link)
    waiting: VecDeque<(u64, std::sync::Weak<link::Link>)>,
    /// the addresses, looked up when the pool opens
    addrs: Result<Vec<Addr>, Fail>,
    /// a new lookup runs on a thread
    resolving: bool,
}

impl Pool {
    fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.load(Ordering::Relaxed))
    }

    /// A connection of this pool closed: one more may open.
    fn lost(&self) {
        let mut st = self.state.lock().unwrap();
        st.open -= 1;
        Self::wake(&st);
    }

    /// Tells the links that wait to try again (through their helpers:
    /// this may run in a leaf call, so no Dart API here).
    fn wake(st: &PoolState) {
        for (_, link) in &st.waiting {
            if let Some(link) = link.upgrade() {
                link.nudge();
            }
        }
    }

    /// The last close: new requests fail, the idle connections close. Each
    /// link closes its own connections once they are idle.
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let idle = {
            let mut st = self.state.lock().unwrap();
            Self::wake(&st);
            std::mem::take(&mut st.idle)
        };
        drop(idle); // after the lock: a drop takes it again
        link::share_all(0);
    }
}

/// Pools by handle. A closed or unknown handle is not found, so a late
/// call from Dart can not touch freed memory. Handles are never used again.
struct Registry {
    next: u64,
    map: HashMap<u64, Arc<Pool>>,
}

static POOLS: Mutex<Option<Registry>> = Mutex::new(None);

fn pools<R>(f: impl FnOnce(&mut Registry) -> R) -> R {
    let mut guard = POOLS.lock().unwrap();
    f(guard.get_or_insert_with(|| Registry {
        next: 0,
        map: HashMap::new(),
    }))
}

fn pool(handle: u64) -> Option<Arc<Pool>> {
    pools(|r| r.map.get(&handle).cloned())
}

// ---- TLS ---------------------------------------------------------------

/// How a pool checks the server certificate, like libpq `sslmode`. Only
/// used when TLS is on (`prefer` or `require` in the config).
#[derive(Clone, Copy, Debug, PartialEq)]
enum Check {
    /// `require`, `prefer`: no check
    None,
    /// `verify-full`: the chain and the host name
    Full,
    /// `verify-ca`: the chain, not the host name
    Chain,
}

impl Check {
    /// The check for the `verify` value from Dart: 0 none, 2 the chain.
    /// Any other value is the full check, so a bad value can not turn the
    /// check off.
    fn of(verify: i64) -> Check {
        match verify {
            0 => Check::None,
            2 => Check::Chain,
            _ => Check::Full,
        }
    }
}

/// The CA certificates to trust. With `root_cert` (a PEM file path), only
/// the certificates in that file, like libpq `sslrootcert`. Without it,
/// the Mozilla roots and the roots of the system.
fn roots(root_cert: &str) -> Result<rustls::RootCertStore, String> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::{self, PemObject};
    let mut roots = rustls::RootCertStore::empty();
    if root_cert.is_empty() {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        // ponytail: a system root that fails to parse is skipped
        for cert in rustls_native_certs::load_native_certs().certs {
            let _ = roots.add(cert);
        }
        return Ok(roots);
    }
    let bad = |e: pem::Error| match e {
        pem::Error::Io(e) => format!("can not read sslRootCert {root_cert}: {e}"),
        e => format!("sslRootCert {root_cert} is not a valid PEM file: {e}"),
    };
    for cert in CertificateDer::pem_file_iter(root_cert).map_err(bad)? {
        roots
            .add(cert.map_err(bad)?)
            .map_err(|e| format!("sslRootCert {root_cert} has a bad certificate: {e}"))?;
    }
    if roots.is_empty() {
        return Err(format!("sslRootCert {root_cert} has no certificate"));
    }
    Ok(roots)
}

/// The TLS config of a pool.
fn tls(check: Check, root_cert: &str) -> Result<rustls::ClientConfig, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("TLS versions");
    Ok(match check {
        Check::Full => builder
            .with_root_certificates(roots(root_cert)?)
            .with_no_client_auth(),
        _ => {
            let roots = match check {
                Check::Chain => Some(Arc::new(roots(root_cert)?)),
                _ => None,
            };
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(Verifier { provider, roots }))
                .with_no_client_auth()
        }
    })
}

/// `require` (no `roots`): any certificate is fine, like libpq.
/// `verify-ca` (`roots`): the chain must end at one of `roots`, the host
/// name is not checked. The handshake signatures are always checked.
#[derive(Debug)]
struct Verifier {
    provider: Arc<rustls::crypto::CryptoProvider>,
    roots: Option<Arc<rustls::RootCertStore>>,
}

impl rustls::client::danger::ServerCertVerifier for Verifier {
    fn verify_server_cert(
        &self,
        cert: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if let Some(roots) = &self.roots {
            rustls::client::verify_server_cert_signed_by_trust_anchor(
                &rustls::server::ParsedCertificate::try_from(cert)?,
                roots,
                intermediates,
                now,
                self.provider.signature_verification_algorithms.all,
            )?;
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

// ---- FFI ---------------------------------------------------------------

/// `len` bytes at `ptr` from Dart. A negative `len` or a null `ptr` is an
/// empty slice: `len as usize` of a negative number is a huge length.
///
/// # Safety
/// When `len` > 0 and `ptr` is not null, `ptr` must point to `len`
/// readable bytes that live for `'a`.
pub(crate) unsafe fn bytes<'a>(ptr: *const u8, len: i64) -> &'a [u8] {
    if ptr.is_null() || len <= 0 {
        return &[];
    }
    unsafe { std::slice::from_raw_parts(ptr, len as usize) }
}

/// ABI version of this library. Dart checks it at load.
#[unsafe(no_mangle)]
pub extern "C" fn dpg_version() -> i32 {
    ABI_VERSION
}

/// Reads `Dart_PostCObject` from `NativeApi.initializeApiDLData`.
/// Returns 0 on success.
///
/// # Safety
/// `data` must be `NativeApi.initializeApiDLData`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_init(data: *mut c_void) -> i32 {
    if data.is_null() {
        return -1;
    }
    let api = unsafe { &*(data as *const DartApi) };
    if api.major != 2 {
        return -2; // DART_API_DL_MAJOR_VERSION
    }
    let mut entry = api.functions;
    unsafe {
        while !(*entry).name.is_null() {
            if CStr::from_ptr((*entry).name).to_bytes() == b"Dart_PostCObject" {
                let f: PostFn = std::mem::transmute((*entry).function);
                let _ = POST.set(f);
                return 0;
            }
            entry = entry.add(1);
        }
    }
    -3
}

/// Makes a pool from a libpq style config (`host=... user=...`).
/// Connections open when they are needed. The same config, `max_size`,
/// `verify` and `root_cert` give the same pool, so isolates share it: up
/// to `max_size` connections for the whole process. Each open needs one
/// [dpg_close]. Returns a pool handle (> 0), -1 when the config is bad, or
/// -2 when the CA file can not be used. For -2 the reason is in `err`
/// (UTF-8, cut at `err_cap` bytes, the rest untouched).
///
/// `verify` checks the server certificate when TLS is on: 0 no check
/// (`require`, `prefer`), 1 the chain and the host name (`verify-full`),
/// 2 the chain only (`verify-ca`). Any other value is `verify-full`. `root_cert` is the path of a PEM file
/// with the CA certificates to trust (`sslrootcert`), or empty for the
/// Mozilla and system roots.
///
/// The host name is looked up here, and again after a connect fails.
///
/// # Safety
/// `cfg` must point to `len` readable bytes, `root_cert` to `root_len`
/// readable bytes (or `root_len` is 0), `err` to `err_cap` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_connect(
    cfg: *const u8,
    len: i64,
    max_size: i64,
    verify: i64,
    root_cert: *const u8,
    root_len: i64,
    err: *mut u8,
    err_cap: i64,
) -> i64 {
    if len < 0 || root_len < 0 {
        return -1;
    }
    let cfg = unsafe { bytes(cfg, len) };
    let root_cert = unsafe { bytes(root_cert, root_len) };
    let (Ok(text), Ok(root_cert)) = (std::str::from_utf8(cfg), std::str::from_utf8(root_cert))
    else {
        return -1;
    };
    let Some(config) = parse_config(text) else {
        return -1;
    };
    let max_size = max_size.max(1) as usize;
    let check = Check::of(verify);
    // sslmode is in `cfg`, so the key has all the TLS settings
    let key = format!("{max_size} {check:?} {root_cert:?} {text}");
    pools(|r| {
        if let Some((handle, pool)) = r.map.iter().find(|(_, p)| p.key == key) {
            pool.refs.fetch_add(1, Ordering::Relaxed);
            return *handle as i64;
        }
        let tls = match tls(check, root_cert) {
            Ok(t) => t,
            Err(text) => {
                let n = text.len().min(err_cap.max(0) as usize);
                unsafe { std::ptr::copy_nonoverlapping(text.as_ptr(), err, n) };
                return -2;
            }
        };
        let addrs = resolve(&config);
        r.next += 1;
        let pool = Pool {
            key,
            refs: AtomicUsize::new(1),
            max: max_size,
            tls: (config.ssl != Ssl::Disable).then(|| Arc::new(tls)),
            cfg: config,
            closed: AtomicBool::new(false),
            wanted: AtomicUsize::new(0),
            timeout_ms: AtomicU64::new(TIMEOUT.as_millis() as u64),
            schema: AtomicU64::new(0),
            state: Mutex::new(PoolState {
                open: 0,
                idle: Vec::new(),
                waiting: VecDeque::new(),
                addrs,
                resolving: false,
            }),
        };
        r.map.insert(r.next, Arc::new(pool));
        r.next as i64
    })
}

/// Gives back one open of a pool. The last one closes it for every
/// isolate. Running queries finish, new ones fail.
#[unsafe(no_mangle)]
pub extern "C" fn dpg_close(pool: i64) {
    let closed = pools(|r| {
        let p = r.map.get(&(pool as u64))?;
        if p.refs.fetch_sub(1, Ordering::Relaxed) > 1 {
            return None;
        }
        r.map.remove(&(pool as u64))
    });
    if let Some(p) = closed {
        p.close();
    }
}

/// For tests: the request and transaction timeout of a pool, in ms.
#[unsafe(no_mangle)]
pub extern "C" fn dpg_set_timeout(pool: i64, ms: i64) {
    if let Some(p) = self::pool(pool as u64) {
        p.timeout_ms.store(ms.max(1) as u64, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: u64, sql: &str, params: &[Option<&str>]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&id.to_le_bytes());
        b.extend_from_slice(&7u64.to_le_bytes());
        b.extend_from_slice(&0u64.to_le_bytes());
        b.extend_from_slice(&[0, 0]);
        b.extend_from_slice(&(params.len() as u16).to_le_bytes());
        b.extend_from_slice(&(sql.len() as u32).to_le_bytes());
        b.extend_from_slice(sql.as_bytes());
        for p in params {
            b.extend_from_slice(&0u32.to_le_bytes());
            b.push(0);
            match p {
                None => b.extend_from_slice(&(-1i32).to_le_bytes()),
                Some(v) => {
                    b.extend_from_slice(&(v.len() as i32).to_le_bytes());
                    b.extend_from_slice(v.as_bytes());
                }
            }
        }
        b
    }

    #[test]
    fn parses_requests() {
        let mut b = request(1, "select $1, $2", &[Some("42"), None]);
        b.extend(request(2, "select 1", &[]));
        let cut = Bytes::copy_from_slice(&b[..40]);
        let b = Bytes::from(b);
        let mut p = 0;
        let a = parse(&b, &mut p).unwrap();
        assert_eq!((a.id, a.pool, a.kind), (1, 7, 0));
        assert_eq!(&a.sql[..], b"select $1, $2");
        assert_eq!(a.params[0].data.as_deref(), Some(&b"42"[..]));
        assert!(a.params[1].data.is_none());
        let c = parse(&b, &mut p).unwrap();
        assert_eq!((c.id, &c.sql[..]), (2, &b"select 1"[..]));
        assert!(parse(&b, &mut p).is_none());
        // a cut record is dropped
        let mut p = 0;
        assert!(parse(&cut, &mut p).is_none());
    }

    #[test]
    fn writes_the_body_length() {
        let mut out = vec![1, 2, 3];
        fail(&mut out, 9, &Fail::text("boom"));
        record(&mut out, 10, 3, &5u64.to_le_bytes());
        // an internal request gets no record
        record(&mut out, INTERNAL, 2, b"none");
        let out = &out[3..];
        assert_eq!(rd64(out, 0), 9);
        assert_eq!(out[8], 2);
        assert_eq!(rd32(out, 9), 4);
        assert_eq!(&out[13..17], b"boom");
        // the next record follows right after
        assert_eq!((rd64(out, 17), out[25], rd32(out, 26)), (10, 3, 8));
        assert_eq!(rd64(out, 30), 5);
        assert_eq!(out.len(), 38);
    }

    #[test]
    fn connect_error_names_host_and_port() {
        let c = parse_config("host=db.local port=6543 user=u password=secret").unwrap();
        let Fail(_, text) = dial_error(&c, "Connection refused");
        let text = String::from_utf8(text).unwrap();
        assert_eq!(
            text,
            "error connecting to server at \"db.local\", port 6543: Connection refused"
        );
        assert!(!text.contains("secret"));
        let Fail(_, text) = tls_error(&c, "server does not support TLS");
        let text = String::from_utf8(text).unwrap();
        assert_eq!(
            text,
            "error performing TLS handshake with server at \"db.local\", port 6543: \
             server does not support TLS"
        );
        assert!(!text.contains("secret"));
    }

    #[test]
    fn reads_the_config() {
        let c = parse_config(
            r"host='/cloudsql/proj:us-central1:db' port=6543 user='u' password='a b\'c\\' dbname=d options='-c TimeZone=UTC' connect_timeout=15 sslmode=require application_name = x",
        )
        .unwrap();
        assert_eq!(c.host, "/cloudsql/proj:us-central1:db");
        assert_eq!(c.port, 6543);
        assert_eq!(c.user, "u");
        assert_eq!(c.password, br"a b'c\");
        assert_eq!(c.dbname.as_deref(), Some("d"));
        assert_eq!(c.options.as_deref(), Some("-c TimeZone=UTC"));
        assert_eq!(c.connect_timeout, Some(Duration::from_secs(15)));
        assert_eq!(c.ssl, Ssl::Require);
        assert_eq!(c.application_name.as_deref(), Some("x"));
        assert!(parse_config("bogus=1").is_none());
        assert!(parse_config("sslmode=sometimes").is_none());
        assert!(parse_config("host='open").is_none());
        assert!(parse_config("port=x").is_none());
    }

    /// A host that starts with `/` is a Unix socket folder, like Google
    /// Cloud SQL `/cloudsql/project:region:instance`. The Dart side sends
    /// it quoted, in the libpq key=value form.
    #[cfg(unix)]
    #[test]
    fn reads_a_unix_socket_host() {
        let cfg = "host='/cloudsql/proj:us-central1:db' port=5432 user='u' dbname='d'";
        let c = parse_config(cfg).unwrap();
        match resolve(&c).unwrap().as_slice() {
            [Addr::Unix(path)] => assert_eq!(
                path.to_str(),
                Some("/cloudsql/proj:us-central1:db/.s.PGSQL.5432")
            ),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn an_unknown_verify_value_is_the_full_check() {
        assert_eq!(Check::of(0), Check::None);
        assert_eq!(Check::of(1), Check::Full);
        assert_eq!(Check::of(2), Check::Chain);
        assert_eq!(Check::of(-1), Check::Full);
        assert_eq!(Check::of(7), Check::Full);
    }

    #[test]
    fn negative_ffi_lengths_are_refused() {
        let cfg = b"host=127.0.0.1 port=1 user=u dbname=neg_test";
        let open = |len, root_len| unsafe {
            dpg_connect(
                cfg.as_ptr(),
                len,
                1,
                0,
                cfg.as_ptr(),
                root_len,
                std::ptr::null_mut(),
                0,
            )
        };
        assert_eq!(open(-1, 0), -1);
        assert_eq!(open(cfg.len() as i64, -5), -1);
        assert!(unsafe { bytes(cfg.as_ptr(), -1) }.is_empty());
        assert!(unsafe { bytes(std::ptr::null(), 4) }.is_empty());
        assert_eq!(unsafe { bytes(cfg.as_ptr(), 4) }, b"host");
    }

    #[test]
    fn same_config_shares_one_pool_until_the_last_close() {
        let cfg = b"host=127.0.0.1 port=1 user=u dbname=close_test";
        let open = |max, verify| unsafe {
            dpg_connect(
                cfg.as_ptr(),
                cfg.len() as i64,
                max,
                verify,
                std::ptr::null(),
                0,
                std::ptr::null_mut(),
                0,
            )
        };
        let (a, b, other) = (open(3, 0), open(3, 0), open(4, 0));
        assert_eq!(a, b);
        assert_ne!(a, other);
        // other TLS settings are another pool
        let (checked, chain) = (open(3, 1), open(3, 2));
        assert_ne!(a, checked);
        assert_ne!(checked, chain);
        dpg_close(checked);
        dpg_close(chain);
        let alive = |h: i64| pool(h as u64).is_some();
        dpg_close(a);
        assert!(alive(a));
        dpg_close(b);
        assert!(!alive(a));
        assert!(alive(other));
        dpg_close(other);
        // one more close does nothing
        dpg_close(a);
    }

    const CA: &str = "-----BEGIN CERTIFICATE-----
MIIBbjCCAROgAwIBAgIUapxcBXf/W5J6jq+bvIvAcm7WjZEwCgYIKoZIzj0EAwIw
DDEKMAgGA1UEAwwBdDAeFw0yNjEwMjUwNDE2NTBaFw0yNjEwMjYwNDE2NTBaMAwx
CjAIBgNVBAMMAXQwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAR3126Iq8l00WNh
puPevZclvBR5QCaVwROgDCnA0EOFIhK/2GZklmsSeKGau7VsoFjztFNKmCGskYNa
Ze/kfYogo1MwUTAdBgNVHQ4EFgQUkocrqtYW7FOgQXVrJP57RRe+6KUwHwYDVR0j
BBgwFoAUkocrqtYW7FOgQXVrJP57RRe+6KUwDwYDVR0TAQH/BAUwAwEB/zAKBggq
hkjOPQQDAgNJADBGAiEAqQU+Zx4LzqU6qcXoeWDifNIOS/U34hwvw8dUaeKvGDwC
IQChrAxPCa3pOVQrngZWVrIwXdxzIjmLn0PAqqeF9RpYFg==
-----END CERTIFICATE-----
";

    #[test]
    fn loads_the_root_cert_file() {
        let dir = std::env::temp_dir().join(format!("dpg_roots_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = |name: &str, text: &str| {
            let path = dir.join(name);
            std::fs::write(&path, text).unwrap();
            path.to_str().unwrap().to_string()
        };
        // one or more certificates, other sections are skipped
        let good = file("good.pem", &format!("{CA}{CA}"));
        assert_eq!(roots(&good).unwrap().len(), 2);
        let err = |path: &str| roots(path).unwrap_err();
        let missing = dir.join("missing.pem");
        let missing = missing.to_str().unwrap();
        assert!(err(missing).starts_with(&format!("can not read sslRootCert {missing}: ")));
        let empty = file("empty.pem", "no pem here\n");
        assert_eq!(
            err(&empty),
            format!("sslRootCert {empty} has no certificate")
        );
        let broken = file(
            "broken.pem",
            "-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n",
        );
        assert!(err(&broken).starts_with(&format!("sslRootCert {broken} is not a valid PEM file")));
        let not_der = file(
            "not_der.pem",
            "-----BEGIN CERTIFICATE-----\naGVsbG8=\n-----END CERTIFICATE-----\n",
        );
        assert!(err(&not_der).starts_with(&format!("sslRootCert {not_der} has a bad certificate")));
        // no file: the Mozilla roots, and the system roots
        assert!(roots("").unwrap().len() > 100);
        // dpg_connect gives -2 and the reason, for a check that uses it
        let cfg = b"host=127.0.0.1 port=1 user=u dbname=roots_test";
        let mut buf = [0u8; 256];
        let h = unsafe {
            dpg_connect(
                cfg.as_ptr(),
                cfg.len() as i64,
                1,
                1,
                empty.as_ptr(),
                empty.len() as i64,
                buf.as_mut_ptr(),
                buf.len() as i64,
            )
        };
        assert_eq!(h, -2);
        assert!(String::from_utf8_lossy(&buf).contains("has no certificate"));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
