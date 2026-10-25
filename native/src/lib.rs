//! Native Postgres driver for Dart (dox_postgres_native).
//!
//! Rust owns the database connections (tokio-postgres, deadpool-postgres).
//! Dart sends queries in batches with one leaf FFI call (`dpg_submit`).
//! Rust runs them on a small tokio runtime and posts the results back in
//! batches to a Dart `ReceivePort` with `Dart_PostCObject`.
//!
//! One pool per config and size, for the whole process: isolates that open
//! the same settings share it. Each open needs one `dpg_close`.
//!
//! Result values stay in the Postgres binary format. Dart decodes them.
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
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, Weak};
use std::task::Poll;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod, StatementCache};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio_postgres::error::{ErrorPosition, SqlState};
use tokio_postgres::types::Format;
use tokio_postgres::types::{FromSql, IsNull, Kind, ToSql, Type, to_sql_checked};
use tokio_postgres::{Row, Statement};
use tokio_postgres_rustls::MakeRustlsConnect;

/// Bump when the FFI functions or the record layout change.
const ABI_VERSION: i32 = 4;
const REQ_HEAD: usize = 32;
const RES_HEAD: usize = 13;
/// Statements kept per connection. Over this, the one used longest ago is
/// closed (LRU).
const CACHE_MAX: usize = 512;
/// A batch of this size or more goes to Dart without a copy (external
/// typed data). A smaller one is copied: for a few bytes a copy is cheaper
/// than a finalizer.
const EXTERNAL_MIN: usize = 64 * 1024;
/// Longest wait for one request (pool slot, connect and query), like the
/// default `queryTimeout` of the `postgres` package. Over it the request
/// fails with a clear error, so Dart never waits forever.
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

type Finalizer = unsafe extern "C" fn(*mut c_void, *mut c_void);

/// `Dart_CObject` for a `kTypedData` or `kExternalTypedData` (Uint8)
/// value. 64-bit layout.
#[repr(C)]
struct DartCObject {
    ty: i32,
    _pad: i32,
    td_type: i32,
    _pad2: i32,
    length: isize,
    values: *const u8,
    /// external typed data only: who owns the bytes, and who frees them
    peer: *mut c_void,
    callback: Option<Finalizer>,
}

type PostFn = unsafe extern "C" fn(i64, *mut DartCObject) -> bool;
static POST: OnceLock<PostFn> = OnceLock::new();

/// Dart calls it when the external bytes of a post are not used any more.
unsafe extern "C" fn free_bytes(_: *mut c_void, peer: *mut c_void) {
    drop(unsafe { Box::from_raw(peer as *mut Vec<u8>) });
}

/// Posts bytes to a Dart port. A big batch moves to Dart without a copy,
/// a small one is copied. False when the port is closed.
fn post(port: i64, data: Vec<u8>) -> bool {
    let Some(f) = POST.get() else { return false };
    let mut obj = DartCObject {
        ty: 7, // Dart_CObject_kTypedData
        _pad: 0,
        td_type: 2, // Dart_TypedData_kUint8
        _pad2: 0,
        length: data.len() as isize,
        values: data.as_ptr(),
        peer: std::ptr::null_mut(),
        callback: None,
    };
    if data.len() < EXTERNAL_MIN {
        return unsafe { f(port, &mut obj) };
    }
    // the Vec moves into the box, its bytes stay where they are
    let peer = Box::into_raw(Box::new(data));
    obj.ty = 8; // Dart_CObject_kExternalTypedData
    obj.peer = peer as *mut c_void;
    obj.callback = Some(free_bytes);
    let sent = unsafe { f(port, &mut obj) };
    if !sent {
        // not sent: the bytes are still ours to free
        drop(unsafe { Box::from_raw(peer) });
    }
    sent
}

// ---- runtime and handles -----------------------------------------------

static RUNTIME: OnceLock<Handle> = OnceLock::new();

/// The runtime of the whole process. The first `dpg_connect` picks its kind:
/// `threads` 0 is one `current_thread` runtime on its own thread, more is a
/// multi-thread runtime with that many workers.
fn runtime(threads: usize) -> &'static Handle {
    RUNTIME.get_or_init(|| {
        if threads == 0 {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            let handle = rt.handle().clone();
            std::thread::Builder::new()
                .name("dox-pg".into())
                .spawn(move || rt.block_on(std::future::pending::<()>()))
                .expect("thread");
            handle
        } else {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads)
                .thread_name("dox-pg")
                .enable_all()
                .build()
                .expect("runtime");
            let handle = rt.handle().clone();
            std::mem::forget(rt); // lives as long as the process
            handle
        }
    })
}

/// Handles given to Dart. A closed or unknown handle is not found, so a
/// late call from Dart can not touch freed memory.
struct Registry<T> {
    next: u64,
    map: HashMap<u64, T>,
}

impl<T: Clone> Registry<T> {
    fn new() -> Self {
        Registry {
            next: 0,
            map: HashMap::new(),
        }
    }
    fn insert(&mut self, value: T) -> u64 {
        self.next += 1;
        self.map.insert(self.next, value);
        self.next
    }
    fn get(&self, key: u64) -> Option<T> {
        self.map.get(&key).cloned()
    }
}

static POOLS: Mutex<Option<Registry<Arc<Shared>>>> = Mutex::new(None);

/// A pool, and the connections that plain queries share right now.
///
/// tokio-postgres pipelines the queries that run at the same time on one
/// connection: it writes them together and reads their answers together.
/// So a plain query does not wait for a free connection. It goes to an
/// idle one, or a new one, or else to the least busy one. Transactions
/// still get a connection of their own from the pool.
struct Shared {
    pool: Pool,
    /// the strong count of each, minus one, is its number of queries
    busy: Mutex<Vec<Arc<Object>>>,
    /// the size and the config: the same key gives the same pool
    key: String,
    /// how many opens use this pool. The last close closes it.
    refs: AtomicUsize,
}

impl Shared {
    async fn lease(&self) -> Result<Lease<'_>, deadpool_postgres::PoolError> {
        {
            let busy = self.busy.lock().unwrap();
            let least = busy
                .iter()
                .filter(|c| !c.is_closed())
                .min_by_key(|c| Arc::strong_count(c));
            let s = self.pool.status();
            let grow = s.available > 0 || s.size < s.max_size;
            if let Some(c) = least {
                // someone waits for the pool (eg. a transaction): do not
                // keep the connections busy, queue up behind it
                if Arc::strong_count(c) == 1 || (!grow && s.waiting == 0) {
                    return Ok(Lease(self, Some(c.clone())));
                }
            }
        }
        let client = Arc::new(self.pool.get().await?);
        self.busy.lock().unwrap().push(client.clone());
        Ok(Lease(self, Some(client)))
    }
}

/// One query on a shared connection. On drop, a connection with no query
/// left goes back to the pool.
struct Lease<'a>(&'a Shared, Option<Arc<Object>>);

impl std::ops::Deref for Lease<'_> {
    type Target = Object;
    fn deref(&self) -> &Object {
        self.1.as_ref().unwrap()
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        let mut busy = self.0.busy.lock().unwrap();
        self.1 = None;
        busy.retain(|c| Arc::strong_count(c) > 1);
    }
}
static SINKS: Mutex<Option<Registry<Arc<Sink>>>> = Mutex::new(None);
static TXS: Mutex<Option<Registry<mpsc::UnboundedSender<Job>>>> = Mutex::new(None);

fn with<T: Clone, R>(reg: &Mutex<Option<Registry<T>>>, f: impl FnOnce(&mut Registry<T>) -> R) -> R {
    let mut guard = reg.lock().unwrap();
    f(guard.get_or_insert_with(Registry::new))
}

// ---- results to Dart ---------------------------------------------------

/// Results of one Dart isolate. Results that are ready in the same runtime
/// tick go to Dart in one post.
struct Sink {
    port: i64,
    buf: Mutex<Vec<u8>>,
    queued: AtomicBool,
}

impl Sink {
    /// Adds the answer to request `id`. It is written straight into the
    /// buffer of the next post.
    fn answer(self: &Arc<Self>, id: u64, answer: Answer) {
        let mut buf = self.buf.lock().unwrap();
        answer.write(&mut buf, id);
        if !self.queued.swap(true, Ordering::AcqRel) {
            let sink = self.clone();
            RUNTIME.get().expect("runtime").spawn(async move {
                // let the other tasks that are ready add their results first
                tokio::task::yield_now().await;
                sink.flush();
            });
        }
    }

    fn flush(&self) {
        let out = {
            let mut buf = self.buf.lock().unwrap();
            self.queued.store(false, Ordering::Release);
            std::mem::take(&mut *buf)
        };
        // false: the isolate is gone. Nobody waits for these results.
        post(self.port, out);
    }
}

/// The answer to one request.
enum Answer {
    Rows(Statement, Vec<Row>),
    /// no columns and no rows (commit, rollback)
    Empty,
    /// a transaction started, with this id
    Begun(u64),
    Db(tokio_postgres::Error),
    Text(String),
}

impl Answer {
    fn text(text: &str) -> Answer {
        Answer::Text(text.to_string())
    }

    /// Adds the result record of request `id` to `out`.
    fn write(&self, out: &mut Vec<u8>, id: u64) {
        match self {
            Answer::Rows(stmt, r) => rows(out, id, stmt, r),
            Answer::Empty => {
                let at = head(out, id, 0);
                out.extend_from_slice(&[0; 6]);
                done(out, at);
            }
            Answer::Begun(key) => {
                let at = head(out, id, 3);
                out.extend_from_slice(&key.to_le_bytes());
                done(out, at);
            }
            Answer::Db(e) => error(out, id, e),
            Answer::Text(text) => message(out, id, text),
        }
    }
}

impl From<deadpool_postgres::PoolError> for Answer {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        match e {
            deadpool_postgres::PoolError::Backend(e) => Answer::Db(e),
            e => Answer::Text(e.to_string()),
        }
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

fn message(out: &mut Vec<u8>, id: u64, text: &str) {
    let at = head(out, id, 2);
    out.extend_from_slice(text.as_bytes());
    done(out, at);
}

fn error(out: &mut Vec<u8>, id: u64, e: &tokio_postgres::Error) {
    let Some(db) = e.as_db_error() else {
        // with the causes, eg. why the TLS handshake failed
        let mut text = e.to_string();
        let mut cause = std::error::Error::source(e);
        while let Some(c) = cause {
            text.push_str(": ");
            text.push_str(&c.to_string());
            cause = c.source();
        }
        return message(out, id, &text);
    };
    let at = head(out, id, 1);
    let line = db.line().map(|l| l.to_string());
    let severity = db.parsed_severity().map(|s| s.to_string());
    let (position, internal, query) = match db.position() {
        Some(ErrorPosition::Original(p)) => (Some(p.to_string()), None, None),
        Some(ErrorPosition::Internal { position, query }) => {
            (None, Some(position.to_string()), Some(query.as_str()))
        }
        None => (None, None, None),
    };
    let fields: [(u8, Option<&str>); 18] = [
        (b'S', Some(db.severity())),
        (b'V', severity.as_deref()),
        (b'C', Some(db.code().code())),
        (b'M', Some(db.message())),
        (b'D', db.detail()),
        (b'H', db.hint()),
        (b'P', position.as_deref()),
        (b'p', internal.as_deref()),
        (b'q', query),
        (b'W', db.where_()),
        (b's', db.schema()),
        (b't', db.table()),
        (b'c', db.column()),
        (b'd', db.datatype()),
        (b'n', db.constraint()),
        (b'F', db.file()),
        (b'L', line.as_deref()),
        (b'R', db.routine()),
    ];
    let count_at = out.len();
    out.push(0);
    let mut count = 0u8;
    for (code, value) in fields {
        let Some(value) = value else { continue };
        out.push(code);
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value.as_bytes());
        count += 1;
    }
    out[count_at] = count;
    done(out, at);
}

/// Any column as its raw bytes (binary format).
struct Raw<'a>(Option<&'a [u8]>);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Raw(Some(raw)))
    }
    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Raw(None))
    }
    fn accepts(_: &Type) -> bool {
        true
    }
}

fn rows(out: &mut Vec<u8>, id: u64, stmt: &Statement, rows: &[Row]) {
    let cols = stmt.columns();
    out.reserve(64 + cols.len() * (16 + rows.len() * 16));
    let at = head(out, id, 0);
    out.extend_from_slice(&(cols.len() as u16).to_le_bytes());
    for c in cols {
        out.extend_from_slice(&c.type_().oid().to_le_bytes());
        out.extend_from_slice(&(c.name().len() as u16).to_le_bytes());
        out.extend_from_slice(c.name().as_bytes());
    }
    out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for row in rows {
        for i in 0..cols.len() {
            match row.try_get::<_, Raw>(i).map(|r| r.0) {
                Ok(Some(v)) => {
                    out.extend_from_slice(&(v.len() as i32).to_le_bytes());
                    out.extend_from_slice(v);
                }
                _ => out.extend_from_slice(&(-1i32).to_le_bytes()),
            }
        }
    }
    done(out, at);
}

// ---- queries -----------------------------------------------------------

/// A parameter, already encoded by Dart. `data` is a slice of the batch.
struct Param {
    oid: u32,
    binary: bool,
    data: Option<Bytes>,
}

impl std::fmt::Debug for Param {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Param({})", self.oid)
    }
}

impl ToSql for Param {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match &self.data {
            None => Ok(IsNull::Yes),
            Some(d) => {
                out.extend_from_slice(d);
                Ok(IsNull::No)
            }
        }
    }
    fn accepts(_: &Type) -> bool {
        true
    }
    fn encode_format(&self, _: &Type) -> Format {
        if self.binary {
            Format::Binary
        } else {
            Format::Text
        }
    }
    to_sql_checked!();
}

/// oid 0: the server picks the type, like an untyped `postgres` parameter.
fn param_type(oid: u32) -> Type {
    Type::from_oid(oid)
        .unwrap_or_else(|| Type::new(String::new(), oid, Kind::Pseudo, String::new()))
}

/// The plan of a cached statement is out of date (eg. after ALTER TABLE).
fn stale(e: &tokio_postgres::Error) -> bool {
    e.code() == Some(&SqlState::FEATURE_NOT_SUPPORTED)
        || e.code() == Some(&SqlState::INVALID_SQL_STATEMENT_NAME)
}

/// Use order of the statements in the cache of one connection.
#[derive(Default)]
struct Lru {
    tick: u64,
    /// hash of (sql, types) to the last use, the sql and the types
    used: HashMap<u64, (u64, Box<str>, Vec<Type>)>,
}

impl Lru {
    /// Marks a statement as used. Gives back the one used longest ago when
    /// more than `max` are kept.
    // ponytail: a 64-bit hash as the key. Two statements with the same
    // hash (about 1 in 2^64) only keep one statement more than `max`.
    fn touch(&mut self, sql: &str, types: &[Type], max: usize) -> Option<(Box<str>, Vec<Type>)> {
        let mut h = DefaultHasher::new();
        (sql, types).hash(&mut h);
        let key = h.finish();
        self.tick += 1;
        if let Some(e) = self.used.get_mut(&key) {
            e.0 = self.tick;
            return None;
        }
        self.used
            .insert(key, (self.tick, sql.into(), types.to_vec()));
        if self.used.len() <= max {
            return None;
        }
        // ponytail: a scan of the cache, only for a new statement over the
        // limit. That one costs a round trip to the server anyway.
        let oldest = *self.used.iter().min_by_key(|(_, e)| e.0)?.0;
        self.used
            .remove(&oldest)
            .map(|(_, sql, types)| (sql, types))
    }
}

/// The [Lru] of each connection, by the address of its statement cache.
/// The `Weak` keeps that address from being used again while it is here.
type Lrus = HashMap<usize, (Weak<StatementCache>, Lru)>;
static LRUS: LazyLock<Mutex<Lrus>> = LazyLock::new(Mutex::default);

/// Marks a statement of `client` as used, and closes the one used longest
/// ago when the cache is full.
fn remember(client: &Object, sql: &str, types: &[Type]) {
    let cache = &client.statement_cache;
    let at = Arc::as_ptr(cache) as usize;
    let mut all = LRUS.lock().unwrap();
    if !all.contains_key(&at) {
        // a new connection: forget the closed ones
        all.retain(|_, (c, _)| c.strong_count() > 0);
    }
    let (_, lru) = all
        .entry(at)
        .or_insert_with(|| (Arc::downgrade(cache), Lru::default()));
    if let Some((sql, types)) = lru.touch(sql, types, CACHE_MAX) {
        cache.remove(&sql, &types);
    }
}

async fn query(client: &Object, sql: &[u8], params: &[Param]) -> Answer {
    let Ok(sql) = std::str::from_utf8(sql) else {
        return Answer::text("dox_postgres_native: the sql is not UTF-8");
    };
    let types: Vec<Type> = if params.iter().any(|p| p.oid != 0) {
        params.iter().map(|p| param_type(p.oid)).collect()
    } else {
        Vec::new()
    };
    let refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as _).collect();
    let mut retried = false;
    loop {
        let stmt = if types.is_empty() {
            client.prepare_cached(sql).await
        } else {
            client.prepare_typed_cached(sql, &types).await
        };
        let stmt = match stmt {
            Ok(s) => s,
            Err(e) => return Answer::Db(e),
        };
        remember(client, sql, &types);
        match client.query(&stmt, &refs).await {
            Ok(r) => return Answer::Rows(stmt, r),
            Err(e) if !retried && stale(&e) => {
                client.statement_cache.remove(sql, &types);
                retried = true;
            }
            Err(e) => return Answer::Db(e),
        }
    }
}

/// Runs `work`, or gives a timeout error after [TIMEOUT].
async fn timed(work: impl Future<Output = Answer>) -> Answer {
    tokio::time::timeout(TIMEOUT, work)
        .await
        .unwrap_or_else(|_| Answer::text(TIMED_OUT))
}

/// Work for the task that owns the connection of one transaction.
enum Job {
    Query {
        id: u64,
        sql: Bytes,
        params: Vec<Param>,
        sink: Arc<Sink>,
    },
    End {
        id: u64,
        commit: bool,
        sink: Arc<Sink>,
    },
}

/// Starts a transaction on one connection. Its queries run one after the
/// other on that connection, in the order Dart sent them.
async fn begin(id: u64, pool: Pool, sink: Arc<Sink>) {
    let start = async {
        let client = pool.get().await.map_err(Answer::from)?;
        match client.batch_execute("BEGIN").await {
            Ok(()) => Ok(client),
            Err(e) => Err(Answer::Db(e)),
        }
    };
    let client = match tokio::time::timeout(TIMEOUT, start).await {
        Ok(Ok(c)) => c,
        Ok(Err(answer)) => return sink.answer(id, answer),
        Err(_) => return sink.answer(id, Answer::text(TIMED_OUT)),
    };
    let (tx, mut rx) = mpsc::unbounded_channel();
    let key = with(&TXS, |r| r.insert(tx));
    sink.answer(id, Answer::Begun(key));
    loop {
        // Dart did not end it in time (eg. its isolate is gone): roll back
        // and give the connection back, so the pool does not run dry.
        let Ok(Some(job)) = tokio::time::timeout(TIMEOUT, rx.recv()).await else {
            close(key, &mut rx);
            let _ = tokio::time::timeout(TIMEOUT, client.batch_execute("ROLLBACK")).await;
            return;
        };
        match job {
            Job::Query {
                id,
                sql,
                params,
                sink,
            } => sink.answer(id, timed(query(&client, &sql, &params)).await),
            Job::End { id, commit, sink } => {
                close(key, &mut rx);
                let sql = if commit { "COMMIT" } else { "ROLLBACK" };
                let answer = timed(async {
                    match client.batch_execute(sql).await {
                        Ok(()) => Answer::Empty,
                        Err(e) => Answer::Db(e),
                    }
                })
                .await;
                return sink.answer(id, answer);
            }
        }
    }
}

/// Ends transaction `key` for new requests (they fail in `run`). The ones
/// already sent (eg. a second rollback) get an answer here.
fn close(key: u64, rx: &mut mpsc::UnboundedReceiver<Job>) {
    with(&TXS, |r| r.map.remove(&key));
    rx.close();
    while let Ok(job) = rx.try_recv() {
        let (Job::Query { id, sink, .. } | Job::End { id, sink, .. }) = job;
        sink.answer(id, Answer::text("the transaction is closed"));
    }
}

fn rd16(b: &[u8], p: usize) -> usize {
    u16::from_le_bytes([b[p], b[p + 1]]) as usize
}

fn rd32(b: &[u8], p: usize) -> u32 {
    u32::from_le_bytes(b[p..p + 4].try_into().unwrap())
}

fn rd64(b: &[u8], p: usize) -> u64 {
    u64::from_le_bytes(b[p..p + 8].try_into().unwrap())
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

/// Runs a request of a transaction, or a begin, right away. Gives back a
/// plain query, to run with the others of its batch in [run_plain].
fn run(req: Request, sink: &Arc<Sink>, rt: &Handle) -> Option<Request> {
    if req.tx == 0 && req.kind == 0 {
        return Some(req);
    }
    let sink = sink.clone();
    let Request {
        id,
        pool,
        tx,
        kind,
        sql,
        params,
    } = req;
    if tx != 0 {
        let Some(sender) = with(&TXS, |r| r.get(tx)) else {
            sink.answer(id, Answer::text("the transaction is closed"));
            return None;
        };
        let job = match kind {
            0 => Job::Query {
                id,
                sql,
                params,
                sink: sink.clone(),
            },
            _ => Job::End {
                id,
                commit: kind == 2,
                sink: sink.clone(),
            },
        };
        if sender.send(job).is_err() {
            sink.answer(id, Answer::text("the transaction is closed"));
        }
        return None;
    }
    let Some(pool) = with(&POOLS, |r| r.get(pool)) else {
        sink.answer(id, Answer::text("the pool is closed"));
        return None;
    };
    match kind {
        1 => {
            rt.spawn(begin(id, pool.pool.clone(), sink));
        }
        _ => sink.answer(id, Answer::text("no transaction to end")),
    }
    None
}

/// The plain queries of one batch from Dart, in one task. They all start
/// in the first poll, so the queries that go to one connection cost one
/// write. Results go to Dart as they come.
///
/// They start together, so one timer for the batch gives each the same
/// [TIMEOUT] as a timer per query, for one timer instead of many.
fn run_plain(reqs: Vec<Request>, sink: Arc<Sink>, rt: &Handle) {
    rt.spawn(async move {
        let ids: Vec<u64> = reqs.iter().map(|r| r.id).collect();
        let mut answered = vec![false; ids.len()];
        let mut running: FuturesUnordered<_> = reqs
            .into_iter()
            .enumerate()
            .map(|(i, r)| async move {
                let Some(pool) = with(&POOLS, |p| p.get(r.pool)) else {
                    return (i, Answer::text("the pool is closed"));
                };
                let answer = match pool.lease().await {
                    Ok(client) => query(&client, &r.sql, &r.params).await,
                    Err(e) => Answer::from(e),
                };
                (i, answer)
            })
            .collect();
        let mut deadline = std::pin::pin!(tokio::time::sleep(TIMEOUT));
        loop {
            // Some(item) for a result or the end, None at the deadline
            let next = std::future::poll_fn(|cx| match running.poll_next_unpin(cx) {
                Poll::Ready(item) => Poll::Ready(Some(item)),
                Poll::Pending => deadline.as_mut().poll(cx).map(|()| None),
            })
            .await;
            match next {
                Some(Some((i, answer))) => {
                    answered[i] = true;
                    sink.answer(ids[i], answer);
                }
                Some(None) => return,
                None => break,
            }
        }
        // dropping `running` stops the queries that are left
        for (id, done) in ids.iter().zip(answered) {
            if !done {
                sink.answer(*id, Answer::text(TIMED_OUT));
            }
        }
    });
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

/// The TLS connector of a pool.
fn tls(check: Check, root_cert: &str) -> Result<MakeRustlsConnect, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("TLS versions");
    let config = match check {
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
    };
    Ok(MakeRustlsConnect::new(config))
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

/// Makes a pool from a libpq style config (`host=... user=...` or a
/// `postgres://` url). Connections open when they are needed. The same
/// config, `max_size`, `verify` and `root_cert` give the same pool, so
/// isolates share it: up to `max_size` connections for the whole process.
/// Each open needs one [dpg_close]. Returns a pool handle (> 0), -1 when
/// the config is bad, or -2 when the CA file can not be used. For -2 the
/// reason is in `err` (UTF-8, cut at `err_cap` bytes, the rest untouched).
///
/// `verify` checks the server certificate when TLS is on: 0 no check
/// (`require`, `prefer`), 1 the chain and the host name (`verify-full`),
/// 2 the chain only (`verify-ca`). `root_cert` is the path of a PEM file
/// with the CA certificates to trust (`sslrootcert`), or empty for the
/// Mozilla and system roots.
///
/// # Safety
/// `cfg` must point to `len` readable bytes, `root_cert` to `root_len`
/// readable bytes (or `root_len` is 0), `err` to `err_cap` writable bytes.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn dpg_connect(
    cfg: *const u8,
    len: i64,
    max_size: i64,
    threads: i64,
    verify: i64,
    root_cert: *const u8,
    root_len: i64,
    err: *mut u8,
    err_cap: i64,
) -> i64 {
    let cfg = unsafe { std::slice::from_raw_parts(cfg, len as usize) };
    let root_cert = if root_len > 0 {
        unsafe { std::slice::from_raw_parts(root_cert, root_len as usize) }
    } else {
        &[]
    };
    let (Ok(cfg), Ok(root_cert)) = (std::str::from_utf8(cfg), std::str::from_utf8(root_cert))
    else {
        return -1;
    };
    let Ok(pg) = tokio_postgres::Config::from_str(cfg) else {
        return -1;
    };
    let max_size = max_size.max(1) as usize;
    let check = match verify {
        1 => Check::Full,
        2 => Check::Chain,
        _ => Check::None,
    };
    // sslmode is in `cfg`, so the key has all the TLS settings
    let key = format!("{max_size} {check:?} {root_cert:?} {cfg}");
    runtime(threads.max(0) as usize);
    with(&POOLS, |r| {
        if let Some((handle, shared)) = r.map.iter().find(|(_, s)| s.key == key) {
            shared.refs.fetch_add(1, Ordering::Relaxed);
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
        let manager = Manager::from_config(
            pg,
            tls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let Ok(pool) = Pool::builder(manager).max_size(max_size).build() else {
            return -1;
        };
        r.insert(Arc::new(Shared {
            pool,
            busy: Mutex::new(Vec::new()),
            key,
            refs: AtomicUsize::new(1),
        })) as i64
    })
}

/// Gives back one open of a pool. The last one closes it for every
/// isolate. Running queries finish, new ones fail.
#[unsafe(no_mangle)]
pub extern "C" fn dpg_close(pool: i64) {
    let closed = with(&POOLS, |r| {
        let shared = r.map.get(&(pool as u64))?;
        if shared.refs.fetch_sub(1, Ordering::Relaxed) > 1 {
            return None;
        }
        r.map.remove(&(pool as u64))
    });
    if let Some(shared) = closed {
        shared.pool.close();
    }
}

/// Opens the result sink of one isolate. Results go to `send_port`.
#[unsafe(no_mangle)]
pub extern "C" fn dpg_open(send_port: i64) -> i64 {
    let sink = Arc::new(Sink {
        port: send_port,
        buf: Mutex::new(Vec::new()),
        queued: AtomicBool::new(false),
    });
    with(&SINKS, |r| r.insert(sink)) as i64
}

/// Closes a sink. Results that come later are dropped.
#[unsafe(no_mangle)]
pub extern "C" fn dpg_release(sink: i64) {
    with(&SINKS, |r| r.map.remove(&(sink as u64)));
}

/// Called from Dart (leaf call) with a batch of request records.
///
/// # Safety
/// `ptr` must point to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_submit(sink: i64, ptr: *const u8, len: i64) {
    let Some(sink) = with(&SINKS, |r| r.get(sink as u64)) else {
        return;
    };
    let Some(rt) = RUNTIME.get() else { return };
    let b = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
    // one copy of the batch: Dart reuses its buffer after this call.
    // Requests keep slices of it.
    let b = Bytes::copy_from_slice(b);
    let mut p = 0;
    let mut plain = Vec::new();
    while let Some(req) = parse(&b, &mut p) {
        plain.extend(run(req, &sink, rt));
    }
    if !plain.is_empty() {
        run_plain(plain, sink, rt);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::NoTls;

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
        message(&mut out, 9, "boom");
        Answer::Begun(5).write(&mut out, 10);
        let out = &out[3..];
        assert_eq!(rd64(out, 0), 9);
        assert_eq!(out[8], 2);
        assert_eq!(rd32(out, 9), 4);
        assert_eq!(&out[13..17], b"boom");
        // the next record follows right after
        assert_eq!((rd64(out, 17), out[25], rd32(out, 26)), (10, 3, 8));
        assert_eq!(rd64(out, 30), 5);
    }

    #[test]
    fn lru_drops_the_statement_used_longest_ago() {
        let mut lru = Lru::default();
        let int8 = [Type::INT8];
        assert_eq!(lru.touch("a", &[], 2), None);
        assert_eq!(lru.touch("b", &int8, 2), None);
        // a is used again, so b is now the oldest
        assert_eq!(lru.touch("a", &[], 2), None);
        let out = lru.touch("c", &[], 2).unwrap();
        assert_eq!((&*out.0, out.1), ("b", int8.to_vec()));
        // the same sql with other types is another statement
        let out = lru.touch("a", &int8, 2).unwrap();
        assert_eq!((&*out.0, out.1.len()), ("a", 0));
        assert_eq!(lru.used.len(), 2);
    }

    #[test]
    fn same_config_shares_one_pool_until_the_last_close() {
        let cfg = b"host=127.0.0.1 port=1 user=u dbname=close_test";
        let open = |max, verify| unsafe {
            dpg_connect(
                cfg.as_ptr(),
                cfg.len() as i64,
                max,
                0,
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
        let alive = |h: i64| with(&POOLS, |r| r.map.contains_key(&(h as u64)));
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
                0,
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

    /// A host that starts with `/` is a Unix socket folder, like Google
    /// Cloud SQL `/cloudsql/project:region:instance`. The Dart side sends
    /// it quoted, in the libpq key=value form.
    #[cfg(unix)]
    #[test]
    fn reads_a_unix_socket_host() {
        use tokio_postgres::config::Host;
        let cfg = "host='/cloudsql/proj:us-central1:db' port=5432 user='u' dbname='d'";
        let pg = tokio_postgres::Config::from_str(cfg).unwrap();
        assert_eq!(
            pg.get_hosts(),
            &[Host::Unix("/cloudsql/proj:us-central1:db".into())]
        );
    }

    /// Needs Postgres (PG_URL, eg. postgres://postgres:postgres@localhost/postgres).
    #[tokio::test]
    async fn runs_a_query() {
        let Ok(url) = std::env::var("PG_URL") else {
            return;
        };
        let pg = tokio_postgres::Config::from_str(&url).unwrap();
        let manager = Manager::new(pg, NoTls);
        let pool = Pool::builder(manager).max_size(2).build().unwrap();
        let client = pool.get().await.unwrap();
        let p = [Param {
            oid: 0,
            binary: false,
            data: Some(Bytes::from_static(b"41")),
        }];
        let mut out = Vec::new();
        query(&client, b"select $1::int8 + 1 as n", &p)
            .await
            .write(&mut out, 5);
        assert_eq!(out[8], 0, "{:?}", String::from_utf8_lossy(&out));
        // 1 column int8 named n, 1 row with 8 bytes
        assert_eq!(rd16(&out, 13), 1);
        assert_eq!(rd32(&out, 15), 20);
        let at = out.len() - 8;
        assert_eq!(i64::from_be_bytes(out[at..].try_into().unwrap()), 42);
        let mut bad = Vec::new();
        query(&client, b"select * from nope_missing", &[])
            .await
            .write(&mut bad, 6);
        assert_eq!(bad[8], 1);
    }
}
