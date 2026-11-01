//! Direct mode (a prototype to measure): plain queries run on the thread
//! of the Dart isolate, like dox_http_native does for HTTP. There is no
//! tokio thread in between. Dart turns it on with `DOX_PG_DIRECT=1`.
//!
//! Each isolate has one link: one mio `Poll` and its own Postgres
//! connections (non-blocking TCP sockets).
//! - `dpg_direct_submit` (leaf call) takes a batch of request records (see
//!   lib.rs). A plain query (kind 0, no transaction) is encoded at once and
//!   written to the connection with the fewest waiting queries. The bytes
//!   the socket does not take wait in the out buffer of the connection, and
//!   go when it is writable. All other records (begin, commit, rollback,
//!   queries in a transaction) and pools with TLS or a Unix socket take the
//!   tokio path, as before.
//! - `dpg_direct_run` (leaf call) does one poll turn that does not wait,
//!   reads the sockets that have data, and writes the result records (see
//!   lib.rs) of the finished queries into Dart's buffer. It returns the
//!   byte count. It never waits.
//! - When queries wait, the idle helper (one small thread per link)
//!   watches the poll file and posts `[1]` to the Dart port when it has an
//!   event. The flag `armed` makes sure only one side tells Dart: the one
//!   that clears it. Every [PROBE] it also checks that the isolate is
//!   alive (an empty post). When it is gone, the link closes its sockets.
//!   Same as the idle helper of dox_http_native (run.rs).
//!
//! A Dart isolate may move to another OS thread between two event loop
//! turns. So all state is `Send` and lives behind the mutex. Nothing uses
//! thread locals.
//!
//! Protocol: the extended query protocol, pipelined. Each query ends with
//! its own Sync, so an error fails only that query. A statement is
//! prepared once per connection (Parse + Describe). Later uses send only
//! Bind + Execute. All result columns come in the binary format. The
//! frontend messages and the auth (md5, SCRAM-SHA-256) come from
//! `postgres-protocol`. The backend messages are read here by hand: a few
//! lines, and no `Bytes` split per message.
//!
//! ponytail: a prototype. Left out on purpose:
//! - The process-wide connection limit. Each isolate opens up to
//!   `max_size` connections of its own. The real version must share the
//!   limit of the pool.
//! - Connect and auth block the isolate, once per new connection.
//! - No LRU for the statement cache, no query timeout, no TLS, no Unix
//!   socket.
//! - The link of a dead isolate keeps its poll file and a few bytes.
use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{ErrorKind, Read, Write};
use std::net::ToSocketAddrs;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes, BytesMut};
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use postgres_protocol::IsNull;
use postgres_protocol::authentication::md5_hash;
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::frontend;
use tokio_postgres::config::{Host, SslMode};

use super::{POOLS, RUNTIME, Request, SINKS, done, head, parse, post, run, run_plain, with};

/// How often the helper checks that the isolate is alive.
const PROBE: Duration = Duration::from_millis(100);
/// Smallest free room in a read buffer before a read.
const READ_ROOM: usize = 16 * 1024;
/// Error codes after which a query is sent once more, with a new Parse:
/// the statement is gone (eg. its Parse failed) or its plan is out of date
/// (eg. after ALTER TABLE).
const GONE: &[u8] = b"26000";
const STALE: &[u8] = b"0A000";

/// A failed query: the status of its result record (1 server error, 2
/// other error) and the body.
#[derive(Clone)]
struct Fail(u8, Vec<u8>);

impl Fail {
    fn text(text: &str) -> Fail {
        Fail(2, text.as_bytes().to_vec())
    }
}

/// Adds a result record with this status and body.
fn record(out: &mut Vec<u8>, id: u64, status: u8, body: &[u8]) {
    let at = head(out, id, status);
    out.extend_from_slice(body);
    done(out, at);
}

fn be16(b: &[u8], p: usize) -> usize {
    u16::from_be_bytes([b[p], b[p + 1]]) as usize
}

fn be32(b: &[u8], p: usize) -> u32 {
    u32::from_be_bytes(b[p..p + 4].try_into().unwrap())
}

/// The next whole backend message at `p`: its tag and its body. None when
/// it is not all there yet.
fn frame(b: &[u8], p: usize) -> Result<Option<(u8, &[u8])>, String> {
    if p + 5 > b.len() {
        return Ok(None);
    }
    let len = be32(b, p + 1) as usize;
    if len < 4 {
        return Err("dox_postgres_native: bad message from the server".into());
    }
    if p + 1 + len > b.len() {
        return Ok(None);
    }
    Ok(Some((b[p], &b[p + 5..p + 1 + len])))
}

/// An ErrorResponse body (u8 code, text, 0 ... then 0) as the body of a
/// status 1 record: u8 count, per field u8 code, u32 length, text.
fn error_body(b: &[u8]) -> Vec<u8> {
    let mut out = vec![0];
    let mut p = 0;
    while p < b.len() && b[p] != 0 && out[0] < u8::MAX {
        let end = b[p + 1..]
            .iter()
            .position(|&c| c == 0)
            .map_or(b.len(), |e| p + 1 + e);
        out.push(b[p]);
        out.extend_from_slice(&((end - p - 1) as u32).to_le_bytes());
        out.extend_from_slice(&b[p + 1..end]);
        out[0] += 1;
        p = end + 1;
    }
    out
}

/// The `C` (code) field of an ErrorResponse body.
fn error_code(b: &[u8]) -> &[u8] {
    let mut p = 0;
    while p < b.len() && b[p] != 0 {
        let end = b[p + 1..]
            .iter()
            .position(|&c| c == 0)
            .map_or(b.len(), |e| p + 1 + e);
        if b[p] == b'C' {
            return &b[p + 1..end];
        }
        p = end + 1;
    }
    b""
}

/// A RowDescription body as the column part of a status 0 record: u16
/// count, per column u32 type oid, u16 name length, name.
fn columns(b: &[u8]) -> Vec<u8> {
    let count = be16(b, 0);
    let mut out = Vec::with_capacity(2 + count * 16);
    out.extend_from_slice(&(count as u16).to_le_bytes());
    let mut p = 2;
    for _ in 0..count {
        let end = b[p..]
            .iter()
            .position(|&c| c == 0)
            .map_or(b.len(), |e| p + e);
        let name = &b[p..end];
        // after the name: table oid u32, column i16, type oid u32, ...
        let oid = be32(b, end + 7);
        out.extend_from_slice(&oid.to_le_bytes());
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(name);
        p = end + 19;
    }
    out
}

/// Adds the values of a DataRow body: per value i32 length (little
/// endian, -1 NULL), bytes.
fn row(out: &mut Vec<u8>, b: &[u8]) {
    let mut p = 2;
    for _ in 0..be16(b, 0) {
        let len = be32(b, p) as i32;
        out.extend_from_slice(&len.to_le_bytes());
        p += 4;
        if len > 0 {
            out.extend_from_slice(&b[p..p + len as usize]);
            p += len as usize;
        }
    }
}

// ---- connections -------------------------------------------------------

/// A prepared statement of one connection.
struct Stmt {
    name: String,
    key: u64,
    sql: Bytes,
    oids: Vec<u32>,
    /// the column part of the result record, from its Describe
    cols: Option<Vec<u8>>,
}

/// A query that waits for its answer.
struct Wait {
    id: u64,
    /// its statement in `stmts`. None: the unnamed statement (a retry)
    stmt: Option<usize>,
    /// it sent the Parse of its statement
    parsing: bool,
    /// the request, for one retry. None for a retry.
    req: Option<Request>,
    /// the columns, for the unnamed statement
    cols: Option<Vec<u8>>,
    rows: Vec<u8>,
    count: u32,
    fail: Option<Fail>,
    retry: bool,
}

struct Conn {
    sock: TcpStream,
    pool: u64,
    /// bytes the socket did not take yet
    out: BytesMut,
    /// waits for writable (only while `out` has bytes)
    writable: bool,
    inb: Vec<u8>,
    // ponytail: grows with each new sql. Add the LRU of lib.rs for the
    // real version.
    stmts: Vec<Stmt>,
    /// hash of (sql, oids) to the index in `stmts`
    cache: HashMap<u64, usize>,
    waits: VecDeque<Wait>,
}

fn key(sql: &[u8], req: &Request) -> u64 {
    let mut h = DefaultHasher::new();
    sql.hash(&mut h);
    for p in &req.params {
        p.oid.hash(&mut h);
    }
    h.finish()
}

impl Conn {
    /// Encodes the query into `out`. `retry`: on the unnamed statement,
    /// with its own Parse.
    fn queue(&mut self, id: u64, req: Request, retry: bool) -> Result<(), Fail> {
        let Ok(sql) = std::str::from_utf8(&req.sql) else {
            return Err(Fail::text("dox_postgres_native: the sql is not UTF-8"));
        };
        let k = key(&req.sql, &req);
        let cached = if retry {
            None
        } else {
            self.cache.get(&k).copied().filter(|&s| {
                let st = &self.stmts[s];
                st.sql == req.sql && st.oids.iter().copied().eq(req.params.iter().map(|p| p.oid))
            })
        };
        let new_name = (!retry && cached.is_none()).then(|| format!("d{}", self.stmts.len()));
        let name = match (cached, &new_name) {
            (Some(s), _) => self.stmts[s].name.as_str(),
            (None, Some(n)) => n.as_str(),
            (None, None) => "",
        };
        let at = self.out.len();
        let parsing = cached.is_none();
        let written = (|| -> Result<(), String> {
            let out = &mut self.out;
            if parsing {
                frontend::parse(name, sql, req.params.iter().map(|p| p.oid), out)
                    .map_err(|e| e.to_string())?;
                frontend::describe(b'S', name, out).map_err(|e| e.to_string())?;
            }
            frontend::bind(
                "",
                name,
                req.params.iter().map(|p| i16::from(p.binary)),
                &req.params,
                |p, buf| match &p.data {
                    None => Ok(IsNull::Yes),
                    Some(d) => {
                        buf.extend_from_slice(d);
                        Ok(IsNull::No)
                    }
                },
                [1],
                out,
            )
            .map_err(|_| "dox_postgres_native: can not encode the parameters".to_string())?;
            frontend::execute("", 0, out).map_err(|e| e.to_string())?;
            frontend::sync(out);
            Ok(())
        })();
        if let Err(e) = written {
            // nothing of this query goes out
            self.out.truncate(at);
            return Err(Fail(2, e.into_bytes()));
        }
        let stmt = match (cached, new_name) {
            (Some(s), _) => Some(s),
            (None, Some(name)) => {
                self.stmts.push(Stmt {
                    name,
                    key: k,
                    sql: req.sql.clone(),
                    oids: req.params.iter().map(|p| p.oid).collect(),
                    cols: None,
                });
                self.cache.insert(k, self.stmts.len() - 1);
                Some(self.stmts.len() - 1)
            }
            (None, None) => None,
        };
        self.waits.push_back(Wait {
            id,
            stmt,
            parsing,
            req: (!retry).then_some(req),
            cols: None,
            rows: Vec::new(),
            count: 0,
            fail: None,
            retry: false,
        });
        Ok(())
    }

    /// Writes `out` until the socket would block. Then it waits for
    /// writable, and only then: a writable event while idle would wake
    /// the helper for nothing.
    fn flush(&mut self, poll: &Poll, token: Token) -> std::io::Result<()> {
        while !self.out.is_empty() {
            match self.sock.write(&self.out) {
                Ok(0) => return Err(ErrorKind::WriteZero.into()),
                Ok(n) => self.out.advance(n),
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    if !self.writable {
                        self.writable = true;
                        let both = Interest::READABLE | Interest::WRITABLE;
                        poll.registry().reregister(&mut self.sock, token, both)?;
                    }
                    return Ok(());
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if self.writable {
            self.writable = false;
            poll.registry()
                .reregister(&mut self.sock, token, Interest::READABLE)?;
        }
        Ok(())
    }

    /// Reads what the socket has, and adds the result records of the
    /// queries that are done to `ready`. Err: the connection is broken.
    fn read(&mut self, ready: &mut Vec<u8>) -> Result<(), String> {
        loop {
            if self.inb.capacity() - self.inb.len() < READ_ROOM {
                self.inb.reserve(READ_ROOM * 4);
            }
            let len = self.inb.len();
            let room = self.inb.capacity() - len;
            // SAFETY: recv writes at most `room` bytes into the spare
            // capacity, and we only keep the bytes it wrote.
            let n = unsafe {
                libc::recv(
                    self.sock.as_raw_fd(),
                    self.inb.as_mut_ptr().add(len) as *mut libc::c_void,
                    room,
                    0,
                )
            };
            if n == 0 {
                return Err("dox_postgres_native: the server closed the connection".into());
            }
            if n < 0 {
                let e = std::io::Error::last_os_error();
                match e.kind() {
                    ErrorKind::WouldBlock => break,
                    ErrorKind::Interrupted => continue,
                    _ => return Err(format!("dox_postgres_native: read failed: {e}")),
                }
            }
            unsafe { self.inb.set_len(len + n as usize) };
            // A short read took all there was. More data later is a new
            // edge (a new event), so no read just to see WouldBlock.
            if (n as usize) < room {
                break;
            }
        }
        let retries = self.replies(ready)?;
        for (id, req) in retries {
            if let Err(f) = self.queue(id, req, true) {
                record(ready, id, f.0, &f.1);
            }
        }
        Ok(())
    }

    /// Reads the whole messages in `inb`. Gives back the queries to send
    /// once more.
    fn replies(&mut self, ready: &mut Vec<u8>) -> Result<Vec<(u64, Request)>, String> {
        let Conn {
            inb,
            stmts,
            cache,
            waits,
            ..
        } = self;
        let mut retries = Vec::new();
        let mut p = 0;
        while let Some((tag, body)) = frame(inb, p)? {
            p += 1 + body.len() + 4;
            match tag {
                // ParameterStatus, Notice, Notification: not for a query
                b'S' | b'N' | b'A' => continue,
                _ => {}
            }
            let Some(w) = waits.front_mut() else {
                // eg. a FATAL error when the server shuts down
                if tag == b'E' {
                    let mut text = b"dox_postgres_native: ".to_vec();
                    text.extend_from_slice(error_code(body));
                    return Err(String::from_utf8_lossy(&text).into_owned());
                }
                return Err("dox_postgres_native: an answer for no query".into());
            };
            match tag {
                b'D' => {
                    row(&mut w.rows, body);
                    w.count += 1;
                }
                b'T' | b'n' => {
                    let cols = if tag == b'T' {
                        columns(body)
                    } else {
                        vec![0, 0]
                    };
                    match w.stmt {
                        Some(s) if w.parsing => stmts[s].cols = Some(cols),
                        _ => w.cols = Some(cols),
                    }
                }
                b'E' => {
                    let code = error_code(body);
                    let stale = code == GONE || code == STALE;
                    if let Some(s) = w.stmt
                        && (w.parsing || stale)
                        && cache.get(&stmts[s].key) == Some(&s)
                    {
                        // its Parse failed, or it is out of date: the next
                        // query prepares it again
                        cache.remove(&stmts[s].key);
                    }
                    w.retry = stale && !w.parsing && w.req.is_some();
                    if w.fail.is_none() {
                        w.fail = Some(Fail(1, error_body(body)));
                    }
                }
                b'Z' => {
                    let mut w = waits.pop_front().unwrap();
                    if w.retry {
                        retries.push((w.id, w.req.take().unwrap()));
                    } else if let Some(f) = &w.fail {
                        record(ready, w.id, f.0, &f.1);
                    } else {
                        let cols = w
                            .cols
                            .as_ref()
                            .or(w.stmt.and_then(|s| stmts[s].cols.as_ref()));
                        match cols {
                            Some(cols) => {
                                let at = head(ready, w.id, 0);
                                ready.extend_from_slice(cols);
                                ready.extend_from_slice(&w.count.to_le_bytes());
                                ready.extend_from_slice(&w.rows);
                                done(ready, at);
                            }
                            None => record(ready, w.id, 2, b"dox_postgres_native: no columns"),
                        }
                    }
                }
                // ParseComplete, ParameterDescription, BindComplete,
                // CommandComplete, EmptyQueryResponse, PortalSuspended
                _ => {}
            }
        }
        inb.drain(..p);
        Ok(retries)
    }
}

/// Opens a connection and logs in. Blocks.
fn connect(pg: &tokio_postgres::Config) -> Result<std::net::TcpStream, Fail> {
    let Some(Host::Tcp(host)) = pg.get_hosts().first() else {
        return Err(Fail::text(
            "dox_postgres_native: direct mode needs a TCP host",
        ));
    };
    let port = pg.get_ports().first().copied().unwrap_or(5432);
    let wait = pg
        .get_connect_timeout()
        .copied()
        .unwrap_or(Duration::from_secs(15));
    let io = |e: std::io::Error| Fail::text(&format!("dox_postgres_native: {host}:{port}: {e}"));
    let mut last = None;
    let mut sock = None;
    for addr in (host.as_str(), port).to_socket_addrs().map_err(io)? {
        match std::net::TcpStream::connect_timeout(&addr, wait) {
            Ok(s) => {
                sock = Some(s);
                break;
            }
            Err(e) => last = Some(e),
        }
    }
    let Some(mut sock) = sock else {
        return Err(io(last.unwrap_or_else(|| ErrorKind::NotFound.into())));
    };
    sock.set_nodelay(true).map_err(io)?;
    sock.set_read_timeout(Some(wait)).map_err(io)?;
    sock.set_write_timeout(Some(wait)).map_err(io)?;

    let user = pg.get_user().unwrap_or("");
    let password = pg.get_password().unwrap_or(b"");
    let mut params = vec![("client_encoding", "UTF8"), ("user", user)];
    if let Some(db) = pg.get_dbname() {
        params.push(("database", db));
    }
    if let Some(o) = pg.get_options() {
        params.push(("options", o));
    }
    if let Some(a) = pg.get_application_name() {
        params.push(("application_name", a));
    }
    let mut buf = BytesMut::new();
    let enc = |e: std::io::Error| Fail::text(&format!("dox_postgres_native: {e}"));
    frontend::startup_message(params, &mut buf).map_err(enc)?;
    sock.write_all(&buf).map_err(io)?;

    let mut inb = Vec::new();
    let mut scram: Option<ScramSha256> = None;
    loop {
        // the next whole message
        let (tag, body) = loop {
            match frame(&inb, 0).map_err(|e| Fail::text(&e))? {
                Some((tag, body)) => break (tag, body.to_vec()),
                None => {
                    let mut chunk = [0u8; 4096];
                    let n = sock.read(&mut chunk).map_err(io)?;
                    if n == 0 {
                        return Err(Fail::text(
                            "dox_postgres_native: the server closed the connection",
                        ));
                    }
                    inb.extend_from_slice(&chunk[..n]);
                }
            }
        };
        inb.drain(..1 + 4 + body.len());
        buf.clear();
        match tag {
            b'R' => {
                let data = &body[4..];
                match be32(&body, 0) {
                    0 => continue,
                    3 => frontend::password_message(password, &mut buf).map_err(enc)?,
                    5 => {
                        let salt = data[..4].try_into().unwrap();
                        let hash = md5_hash(user.as_bytes(), password, salt);
                        frontend::password_message(hash.as_bytes(), &mut buf).map_err(enc)?;
                    }
                    // ponytail: assumes SCRAM-SHA-256 is in the list. It
                    // always is without TLS (only -PLUS needs TLS).
                    10 => {
                        let s = ScramSha256::new(password, ChannelBinding::unsupported());
                        frontend::sasl_initial_response("SCRAM-SHA-256", s.message(), &mut buf)
                            .map_err(enc)?;
                        scram = Some(s);
                    }
                    11 | 12 => {
                        let Some(s) = scram.as_mut() else {
                            return Err(Fail::text("dox_postgres_native: SASL out of order"));
                        };
                        if be32(&body, 0) == 11 {
                            s.update(data).map_err(enc)?;
                            frontend::sasl_response(s.message(), &mut buf).map_err(enc)?;
                        } else {
                            s.finish(data).map_err(enc)?;
                        }
                    }
                    n => {
                        return Err(Fail::text(&format!(
                            "dox_postgres_native: auth method {n} is not supported in direct mode"
                        )));
                    }
                }
                if !buf.is_empty() {
                    sock.write_all(&buf).map_err(io)?;
                }
            }
            b'E' => return Err(Fail(1, error_body(&body))),
            b'Z' => break,
            // ParameterStatus, BackendKeyData, Notice
            _ => {}
        }
    }
    sock.set_read_timeout(None).map_err(io)?;
    sock.set_write_timeout(None).map_err(io)?;
    sock.set_nonblocking(true).map_err(io)?;
    Ok(sock)
}

// ---- the link of one isolate -------------------------------------------

/// A pool in direct mode: its config, its size, and its connections in
/// this link.
struct Target {
    pg: tokio_postgres::Config,
    max: usize,
    conns: Vec<usize>,
}

struct Core {
    poll: Poll,
    events: Events,
    /// by token
    conns: Vec<Option<Conn>>,
    /// by pool handle. None: the pool takes the tokio path (TLS, Unix
    /// socket).
    pools: HashMap<u64, Option<Target>>,
    /// result records not given to Dart yet
    ready: Vec<u8>,
}

impl Core {
    fn waiting(&self) -> bool {
        self.conns
            .iter()
            .flatten()
            .any(|c| !c.waits.is_empty() || !c.out.is_empty())
    }

    /// true when plain queries of `pool` run here. `checked`: the pools
    /// already seen in this batch, so the global pool list is read once
    /// per pool and batch (a closed pool takes the tokio path, which
    /// answers "the pool is closed").
    fn direct(&mut self, pool: u64, checked: &mut Vec<u64>) -> bool {
        if !checked.contains(&pool) {
            checked.push(pool);
            match with(&POOLS, |r| r.get(pool)) {
                None => {
                    self.pools.remove(&pool);
                    return false;
                }
                Some(shared) => {
                    self.pools.entry(pool).or_insert_with(|| {
                        let pg = shared.pg.clone();
                        let tcp = matches!(pg.get_hosts().first(), Some(Host::Tcp(_)));
                        (tcp && pg.get_ssl_mode() == SslMode::Disable).then(|| Target {
                            pg,
                            max: shared.pool.status().max_size,
                            conns: Vec::new(),
                        })
                    });
                }
            }
        }
        matches!(self.pools.get(&pool), Some(Some(_)))
    }

    /// Sends a plain query of a direct pool. A connect error fails it, and
    /// the next queries of this batch for that pool (`failed`).
    fn send(&mut self, req: Request, failed: &mut HashMap<u64, Fail>) {
        let (id, pool) = (req.id, req.pool);
        let Some(Some(t)) = self.pools.get(&pool) else {
            return;
        };
        let best = t
            .conns
            .iter()
            .copied()
            .min_by_key(|&i| self.conns[i].as_ref().map_or(usize::MAX, |c| c.waits.len()));
        let busy = best.is_none_or(|i| self.conns[i].as_ref().is_none_or(|c| !c.waits.is_empty()));
        let i = if busy && t.conns.len() < t.max && !failed.contains_key(&pool) {
            match self.open(pool) {
                Ok(i) => i,
                Err(f) => {
                    failed.insert(pool, f);
                    match best {
                        Some(i) => i,
                        None => {
                            let f = &failed[&pool];
                            return record(&mut self.ready, id, f.0, &f.1);
                        }
                    }
                }
            }
        } else {
            match best {
                Some(i) => i,
                None => {
                    let f = &failed[&pool];
                    return record(&mut self.ready, id, f.0, &f.1);
                }
            }
        };
        let conn = self.conns[i].as_mut().unwrap();
        if let Err(f) = conn.queue(id, req, false) {
            record(&mut self.ready, id, f.0, &f.1);
        }
    }

    /// Opens a new connection of `pool`. Gives back its token.
    fn open(&mut self, pool: u64) -> Result<usize, Fail> {
        let Some(Some(t)) = self.pools.get(&pool) else {
            return Err(Fail::text("the pool is closed"));
        };
        let sock = connect(&t.pg)?;
        let i = self
            .conns
            .iter()
            .position(Option::is_none)
            .unwrap_or(self.conns.len());
        let mut sock = TcpStream::from_std(sock);
        self.poll
            .registry()
            .register(&mut sock, Token(i), Interest::READABLE)
            .map_err(|e| Fail::text(&format!("dox_postgres_native: {e}")))?;
        let conn = Conn {
            sock,
            pool,
            out: BytesMut::with_capacity(16 * 1024),
            writable: false,
            inb: Vec::with_capacity(READ_ROOM * 4),
            stmts: Vec::new(),
            cache: HashMap::new(),
            waits: VecDeque::new(),
        };
        if i == self.conns.len() {
            self.conns.push(Some(conn));
        } else {
            self.conns[i] = Some(conn);
        }
        if let Some(Some(t)) = self.pools.get_mut(&pool) {
            t.conns.push(i);
        }
        Ok(i)
    }

    /// Closes connection `i`. Its waiting queries fail with `why`.
    fn kill(&mut self, i: usize, why: &str) {
        let Some(mut conn) = self.conns[i].take() else {
            return;
        };
        let _ = self.poll.registry().deregister(&mut conn.sock);
        for w in conn.waits {
            record(&mut self.ready, w.id, 2, why.as_bytes());
        }
        if let Some(Some(t)) = self.pools.get_mut(&conn.pool) {
            t.conns.retain(|&c| c != i);
        }
    }

    /// Writes the out buffers that have bytes and do not wait for
    /// writable.
    fn flush_all(&mut self) {
        for i in 0..self.conns.len() {
            let Some(c) = self.conns[i].as_mut() else {
                continue;
            };
            if c.out.is_empty() || c.writable {
                continue;
            }
            if let Err(e) = c.flush(&self.poll, Token(i)) {
                self.kill(i, &format!("dox_postgres_native: write failed: {e}"));
            }
        }
    }

    /// One poll turn that does not wait: writes and reads what the events
    /// say.
    fn turn(&mut self) {
        if self
            .poll
            .poll(&mut self.events, Some(Duration::ZERO))
            .is_err()
        {
            return;
        }
        let mut broken: Vec<(usize, String)> = Vec::new();
        for ev in self.events.iter() {
            let i = ev.token().0;
            let Some(c) = self.conns.get_mut(i).and_then(Option::as_mut) else {
                continue;
            };
            if (ev.is_readable() || ev.is_read_closed() || ev.is_error())
                && let Err(e) = c.read(&mut self.ready)
            {
                broken.push((i, e));
                continue;
            }
            // a writable event, or retries from the read
            if (ev.is_writable() || !c.out.is_empty())
                && let Err(e) = c.flush(&self.poll, Token(i))
            {
                broken.push((i, format!("dox_postgres_native: write failed: {e}")));
            }
        }
        for (i, why) in broken {
            self.kill(i, &why);
        }
    }
}

/// The direct link of one isolate. Dart holds an `Arc` of it as handle.
pub(crate) struct Link {
    core: Mutex<Core>,
    /// the poll file (epoll or kqueue), watched by the idle helper
    fd: i32,
    /// queries wait and Dart is idle: the helper watches, and tells Dart
    /// when an answer comes
    armed: AtomicBool,
    /// wakes the helper: Dart went idle
    kick: Mutex<bool>,
    cv: Condvar,
    stopped: AtomicBool,
    port: i64,
}

impl Link {
    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
        *self.kick.lock().unwrap() = true;
        self.cv.notify_one();
    }

    /// true when the poll file has an event within `wait`. Does not read
    /// the event and does not take the core: `dpg_direct_run` does that.
    fn ready_within(&self, wait: Duration) -> bool {
        let mut p = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let ms = wait.as_millis().min(i32::MAX as u128) as i32;
        let r = unsafe { libc::poll(&mut p, 1, ms) };
        r > 0 && p.revents & libc::POLLIN != 0
    }

    /// The isolate is gone: close the sockets.
    fn gone(&self) {
        self.stopped.store(true, Ordering::Release);
        let mut core = self.core.lock().unwrap();
        core.conns.clear();
        core.pools.clear();
    }

    /// The idle helper: watches the poll file while Dart is idle and
    /// queries wait. Also checks every [PROBE] that the isolate is alive.
    fn helper(&self) {
        let mut probed = Instant::now();
        loop {
            {
                let mut k = self.kick.lock().unwrap();
                while !*k {
                    k = self.cv.wait_timeout(k, PROBE).unwrap().0;
                    if !*k && probed.elapsed() >= PROBE {
                        probed = Instant::now();
                        if !post(self.port, Vec::new()) {
                            drop(k);
                            return self.gone();
                        }
                    }
                }
                *k = false;
            }
            while self.armed.load(Ordering::Acquire) {
                if self.ready_within(PROBE) {
                    // only the side that clears `armed` tells Dart
                    if self
                        .armed
                        .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                        && !post(self.port, vec![1])
                    {
                        return self.gone();
                    }
                    break;
                }
                if probed.elapsed() >= PROBE {
                    probed = Instant::now();
                    if !post(self.port, Vec::new()) {
                        return self.gone();
                    }
                }
            }
        }
    }
}

// ---- FFI ---------------------------------------------------------------

/// Opens the direct link of one isolate. Its idle helper posts to
/// `send_port`. Returns a handle (> 0), or 0 when it can not (then Dart
/// uses the tokio path).
#[unsafe(no_mangle)]
pub extern "C" fn dpg_direct_open(send_port: i64) -> i64 {
    let Ok(poll) = Poll::new() else { return 0 };
    let fd = poll.as_raw_fd();
    let link = Arc::new(Link {
        core: Mutex::new(Core {
            poll,
            events: Events::with_capacity(256),
            conns: Vec::new(),
            pools: HashMap::new(),
            ready: Vec::new(),
        }),
        fd,
        armed: AtomicBool::new(false),
        kick: Mutex::new(false),
        cv: Condvar::new(),
        stopped: AtomicBool::new(false),
        port: send_port,
    });
    let for_helper = link.clone();
    if std::thread::Builder::new()
        .name("dox-pg-idle".into())
        .spawn(move || for_helper.helper())
        .is_err()
    {
        return 0;
    }
    Arc::into_raw(link) as i64
}

/// Like `dpg_submit`: plain queries of a direct pool go out on the
/// sockets of this link at once. The others take the tokio path, and their
/// results come to `sink`.
///
/// # Safety
/// `link` must come from `dpg_direct_open`. `ptr` must point to `len`
/// readable bytes. Only one call per link at a time (one isolate).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_direct_submit(link: i64, sink: i64, ptr: *const u8, len: i64) {
    let s = unsafe { &*(link as *const Link) };
    if s.stopped.load(Ordering::Acquire) {
        return;
    }
    let b = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
    // one copy of the batch: Dart reuses its buffer after this call.
    // Requests keep slices of it (for a retry).
    let b = Bytes::copy_from_slice(b);
    let mut p = 0;
    let mut old = Vec::new();
    let mut checked = Vec::new();
    let mut failed = HashMap::new();
    let mut core = s.core.lock().unwrap();
    while let Some(req) = parse(&b, &mut p) {
        if req.tx == 0 && req.kind == 0 && core.direct(req.pool, &mut checked) {
            core.send(req, &mut failed);
        } else {
            old.push(req);
        }
    }
    core.flush_all();
    let has_ready = !core.ready.is_empty();
    let waiting = core.waiting();
    drop(core);
    if has_ready {
        // eg. a connect error: Dart reads it with its next run
        post(s.port, vec![1]);
    } else if waiting && !s.armed.load(Ordering::Acquire) {
        s.arm();
    }
    if !old.is_empty() {
        let (Some(sink), Some(rt)) = (with(&SINKS, |r| r.get(sink as u64)), RUNTIME.get()) else {
            return;
        };
        let mut plain = Vec::new();
        for req in old {
            plain.extend(run(req, &sink, rt));
        }
        if !plain.is_empty() {
            run_plain(plain, sink, rt);
        }
    }
}

/// Reads the sockets of this link and writes the result records of the
/// finished queries to `buf`. Never waits. Returns the byte count (0:
/// nothing yet, the idle helper tells Dart when an answer comes), or -n:
/// the results need a buffer of at least n bytes (call again with a
/// bigger one).
///
/// # Safety
/// `link` must come from `dpg_direct_open`. `buf` must point to `cap`
/// writable bytes. Only one call per link at a time (one isolate).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_direct_run(link: i64, buf: *mut u8, cap: i64) -> i64 {
    let s = unsafe { &*(link as *const Link) };
    let mut core = s.core.lock().unwrap();
    s.armed.store(false, Ordering::Release);
    core.turn();
    let n = core.ready.len();
    if n > cap.max(0) as usize {
        return -(n as i64);
    }
    unsafe { std::ptr::copy_nonoverlapping(core.ready.as_ptr(), buf, n) };
    core.ready.clear();
    if core.ready.capacity() > 1 << 20 {
        core.ready = Vec::new(); // do not keep a big buffer forever
    }
    let waiting = core.waiting();
    drop(core);
    if waiting {
        s.arm();
    }
    n as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_backend_messages() {
        // RowDescription with one int4 column "n", a DataRow with 42 and
        // one with NULL
        let mut t = vec![0, 1];
        t.extend_from_slice(b"n\0");
        t.extend_from_slice(&[0, 0, 0, 0, 0, 0]); // table oid, column
        t.extend_from_slice(&23u32.to_be_bytes());
        t.extend_from_slice(&[0, 4, 0, 0, 0, 0, 0, 1]); // size, mod, format
        assert_eq!(columns(&t), [1, 0, 23, 0, 0, 0, 1, 0, b'n']);
        let mut out = Vec::new();
        row(&mut out, &[0, 1, 0, 0, 0, 4, 0, 0, 0, 42]);
        row(&mut out, &[0, 1, 255, 255, 255, 255]);
        assert_eq!(out, [4, 0, 0, 0, 0, 0, 0, 42, 255, 255, 255, 255]);
        let e = b"SERROR\0C42P01\0Mno table\0\0";
        assert_eq!(error_code(e), b"42P01");
        let body = error_body(e);
        assert_eq!(body[0], 3);
        assert_eq!(&body[1..6], &[b'S', 5, 0, 0, 0]);
        // a message cut in the middle is not read yet
        let msg = [b'Z', 0, 0, 0, 5, b'I'];
        assert_eq!(frame(&msg, 0).unwrap(), Some((b'Z', &b"I"[..])));
        assert_eq!(frame(&msg[..5], 0).unwrap(), None);
    }
}
