//! One Postgres connection. Its link (link.rs) drives it from the thread
//! of the Dart isolate. Nothing here blocks: every step goes as far as the
//! socket allows, and the next event of the poll goes on.
//!
//! Steps of a new connection:
//! 1. `Connecting`: a non-blocking TCP or Unix socket connect. When it
//!    fails, the next address of the host is tried.
//! 2. `SslReq` (only with TLS): the SSLRequest is sent, the server answers
//!    one byte. `S` starts rustls, `N` is an error for `require` and plain
//!    text for `prefer`.
//! 3. `Auth`: the startup message, then the login: trust, cleartext, md5
//!    or SCRAM-SHA-256 (`postgres-protocol`).
//! 4. `Ready`: requests placed on the connection before this point
//!    (`pre`) are written now.
//!
//! Queries use the extended query protocol, pipelined: they are written
//! at once, many may wait for their answers. Each query ends with its own
//! Sync, so an error fails only that query. A statement is prepared once
//! per connection (Parse + Describe). Later uses send only Bind + Execute.
//! At most [CACHE_MAX] statements are kept; for a new one over that, the
//! one used longest ago is closed (LRU). All result columns come in the
//! binary format. The backend messages are read here by hand: a few lines,
//! and no copy per message.
//!
//! With TLS, the protocol bytes go through a rustls `ClientConnection`
//! (`writer`, `write_tls`, `read_tls`, `reader`).
use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{ErrorKind, Read, Write};
use std::sync::Arc;
use std::time::Instant;

use bytes::{Buf, Bytes, BytesMut};
use mio::net::TcpStream;
#[cfg(unix)]
use mio::net::UnixStream;
use mio::{Interest, Registry, Token};
use postgres_protocol::IsNull;
use postgres_protocol::authentication::md5_hash;
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::frontend;

use crate::{
    Addr, CACHE_MAX, Fail, INTERNAL, Pool, Request, Ssl, TIMED_OUT, done, fail, head, record,
    refresh,
};

/// Smallest free room in a read buffer before a read.
const READ_ROOM: usize = 16 * 1024;
/// An empty read buffer bigger than this is made small again.
const BIG_BUF: usize = 1 << 20;
/// Error codes after which a query is sent once more, with a new Parse:
/// the statement is gone (eg. its Parse failed) or its plan is out of date
/// (eg. after ALTER TABLE).
const GONE: &[u8] = b"26000";
const STALE: &[u8] = b"0A000";

fn be16(b: &[u8], p: usize) -> usize {
    u16::from_be_bytes([b[p], b[p + 1]]) as usize
}

fn be32(b: &[u8], p: usize) -> u32 {
    u32::from_be_bytes(b[p..p + 4].try_into().unwrap())
}

/// The next whole backend message at `p`: its tag and its body. None when
/// it is not all there yet.
fn frame(b: &[u8], p: usize) -> Result<Option<(u8, &[u8])>, Fail> {
    if p + 5 > b.len() {
        return Ok(None);
    }
    let len = be32(b, p + 1) as usize;
    if len < 4 {
        return Err(Fail::text(
            "dox_postgres_native: bad message from the server",
        ));
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

fn connect_error(e: impl std::fmt::Display) -> Fail {
    Fail::text(&format!("error connecting to server: {e}"))
}

fn write_error(e: impl std::fmt::Display) -> Fail {
    Fail::text(&format!("error communicating with the server: {e}"))
}

// ---- requests ----------------------------------------------------------

/// What a request asks a connection to do.
pub(crate) enum Job {
    Query(Request),
    /// start a transaction with this id
    Begin(u64),
    /// end the transaction: true commit, false rollback
    End(bool),
    /// roll back a transaction that timed out (no answer to Dart)
    Rollback,
}

/// A request and when it came.
pub(crate) struct Pending {
    pub id: u64,
    pub job: Job,
    pub at: Instant,
}

enum Kind {
    Query {
        /// its statement in `stmts`. None: the unnamed statement (a retry)
        stmt: Option<u64>,
        /// it sent the Parse of its statement
        parsing: bool,
        /// the request, for one retry. None for a retry.
        req: Option<Request>,
    },
    Begin(u64),
    End,
}

/// A request that waits for its answer.
struct Wait {
    id: u64,
    kind: Kind,
    at: Instant,
    /// the columns, from the Describe of this query
    cols: Option<Vec<u8>>,
    rows: Vec<u8>,
    count: u32,
    fail: Option<Fail>,
    retry: bool,
}

/// A prepared statement of one connection.
struct Stmt {
    name: String,
    sql: Bytes,
    oids: Vec<u32>,
    /// the column part of the result record, from its Describe
    cols: Option<Vec<u8>>,
    /// the tick of its last use, for the LRU
    used: u64,
}

/// The key of a statement in the cache: a hash of the sql and the types.
// ponytail: a 64-bit hash. Two statements with the same hash (about 1 in
// 2^64) take turns in the cache: the one there is closed.
fn key(req: &Request) -> u64 {
    let mut h = DefaultHasher::new();
    req.sql.hash(&mut h);
    for p in &req.params {
        p.oid.hash(&mut h);
    }
    h.finish()
}

/// The statement to close before a new one with key `k` goes into
/// `stmts`: the old one with the same key (another sql, same hash), or
/// the one used longest ago when `max` are kept.
fn evict(stmts: &mut HashMap<u64, Stmt>, k: u64, max: usize) -> Option<(u64, Stmt)> {
    if let Some(s) = stmts.remove(&k) {
        return Some((k, s));
    }
    if stmts.len() < max {
        return None;
    }
    // ponytail: a scan of the cache, only for a new statement over the
    // limit. That one costs a round trip to the server anyway.
    let oldest = *stmts.iter().min_by_key(|(_, s)| s.used)?.0;
    stmts.remove(&oldest).map(|s| (oldest, s))
}

// ---- sockets -----------------------------------------------------------

enum Sock {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl Sock {
    fn dial(addr: &Addr) -> std::io::Result<Sock> {
        match addr {
            Addr::Tcp(a) => TcpStream::connect(*a).map(Sock::Tcp),
            #[cfg(unix)]
            Addr::Unix(p) => UnixStream::connect(p).map(Sock::Unix),
        }
    }

    /// Ok(true): connected. Ok(false): not yet.
    fn connected(&self) -> std::io::Result<bool> {
        let (err, peer) = match self {
            Sock::Tcp(s) => (s.take_error(), s.peer_addr().map(|_| ())),
            #[cfg(unix)]
            Sock::Unix(s) => (s.take_error(), s.peer_addr().map(|_| ())),
        };
        if let Some(e) = err? {
            return Err(e);
        }
        match peer {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotConnected => Ok(false),
            Err(e) => Err(e),
        }
    }
}

impl Read for Sock {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Sock::Tcp(s) => s.read(b),
            #[cfg(unix)]
            Sock::Unix(s) => s.read(b),
        }
    }
}

impl Write for Sock {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        match self {
            Sock::Tcp(s) => s.write(b),
            #[cfg(unix)]
            Sock::Unix(s) => s.write(b),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl mio::event::Source for Sock {
    fn register(&mut self, r: &Registry, t: Token, i: Interest) -> std::io::Result<()> {
        match self {
            Sock::Tcp(s) => s.register(r, t, i),
            #[cfg(unix)]
            Sock::Unix(s) => s.register(r, t, i),
        }
    }
    fn reregister(&mut self, r: &Registry, t: Token, i: Interest) -> std::io::Result<()> {
        match self {
            Sock::Tcp(s) => s.reregister(r, t, i),
            #[cfg(unix)]
            Sock::Unix(s) => s.reregister(r, t, i),
        }
    }
    fn deregister(&mut self, r: &Registry) -> std::io::Result<()> {
        match self {
            Sock::Tcp(s) => s.deregister(r),
            #[cfg(unix)]
            Sock::Unix(s) => s.deregister(r),
        }
    }
}

// ---- the connection ----------------------------------------------------

enum State {
    Connecting,
    SslReq,
    Auth(Option<Box<ScramSha256>>),
    Ready,
}

pub(crate) struct Conn {
    pub pool: Arc<Pool>,
    sock: Sock,
    tls: Option<Box<rustls::ClientConnection>>,
    state: State,
    /// the addresses of the host, and the next one to try
    addrs: Vec<Addr>,
    next_addr: usize,
    /// connect and login must be done by then
    until: Instant,
    /// bytes not written yet (plain text, before TLS)
    out: BytesMut,
    /// registered for writable (while connecting, or `out` is stuck)
    writable: bool,
    /// read buffer: `inb[..filled]` has data
    inb: Vec<u8>,
    filled: usize,
    stmts: HashMap<u64, Stmt>,
    tick: u64,
    names: u64,
    waits: VecDeque<Wait>,
    /// requests placed before the login is done
    pre: Vec<Pending>,
    /// the transaction that owns this connection
    pub tx: Option<u64>,
    /// the last answer of a transaction, for its idle timeout
    last: Instant,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.pool.lost();
    }
}

impl Conn {
    /// Starts a connection to the first address that takes a connect.
    /// The caller has counted it in `open` of the pool; on Err it must
    /// give that back.
    pub fn start(pool: Arc<Pool>, addrs: Vec<Addr>, now: Instant) -> Result<Conn, Fail> {
        let mut last = None;
        for (i, a) in addrs.iter().enumerate() {
            match Sock::dial(a) {
                Ok(sock) => {
                    let wait = pool.cfg.connect_timeout.unwrap_or_else(|| pool.timeout());
                    return Ok(Conn {
                        until: now + wait,
                        pool,
                        sock,
                        tls: None,
                        state: State::Connecting,
                        next_addr: i + 1,
                        addrs,
                        out: BytesMut::with_capacity(16 * 1024),
                        writable: true,
                        inb: Vec::new(),
                        filled: 0,
                        stmts: HashMap::new(),
                        tick: 0,
                        names: 0,
                        waits: VecDeque::new(),
                        pre: Vec::new(),
                        tx: None,
                        last: now,
                    });
                }
                Err(e) => last = Some(e),
            }
        }
        Err(connect_error(last.map_or_else(
            || "no address".to_string(),
            |e| e.to_string(),
        )))
    }

    fn interest(&self) -> Interest {
        if self.writable {
            Interest::READABLE | Interest::WRITABLE
        } else {
            Interest::READABLE
        }
    }

    pub fn register(&mut self, reg: &Registry, token: Token) -> std::io::Result<()> {
        let i = self.interest();
        reg.register(&mut self.sock, token, i)
    }

    pub fn deregister(&mut self, reg: &Registry) {
        let _ = reg.deregister(&mut self.sock);
    }

    /// Requests that wait on this connection.
    pub fn load(&self) -> usize {
        self.waits.len() + self.pre.len()
    }

    /// Logged in, nothing waits, no transaction.
    pub fn idle(&self) -> bool {
        matches!(self.state, State::Ready)
            && self.waits.is_empty()
            && self.pre.is_empty()
            && self.tx.is_none()
    }

    fn need_write(&self) -> bool {
        !self.out.is_empty() || self.tls.as_ref().is_some_and(|t| t.wants_write())
    }

    /// The next time something is due: the end of connect, the timeout of
    /// the oldest request, or the idle timeout of a transaction.
    pub fn due(&self) -> Option<Instant> {
        if !matches!(self.state, State::Ready) {
            return Some(self.until);
        }
        let to = self.pool.timeout();
        match self.waits.front() {
            Some(w) => Some(w.at + to),
            None => self.tx.map(|_| self.last + to),
        }
    }

    /// Does what is due at `now`. Err: the connection must close, its
    /// requests fail with this.
    pub fn expire(&mut self, now: Instant, ready: &mut Vec<u8>) -> Result<(), Fail> {
        if !matches!(self.state, State::Ready) {
            if now >= self.until {
                refresh(&self.pool);
                return Err(connect_error("connection timed out"));
            }
            return Ok(());
        }
        let to = self.pool.timeout();
        if let Some(w) = self.waits.front() {
            if w.at + to <= now {
                return Err(Fail::text(TIMED_OUT));
            }
            return Ok(());
        }
        // Dart did not end the transaction in time (eg. its isolate is
        // gone): roll it back. The connection is free after it.
        if self.tx.is_some() && self.last + to <= now {
            self.tx = None;
            let job = Job::Rollback;
            self.send(
                Pending {
                    id: INTERNAL,
                    job,
                    at: now,
                },
                ready,
            );
        }
        Ok(())
    }

    /// Fails all requests of this connection with `f`. A request that
    /// already has an error from the server (eg. 57P01 when an admin ends
    /// the connection) fails with that one.
    pub fn fail_all(&mut self, f: &Fail, ready: &mut Vec<u8>) {
        for w in self.waits.drain(..) {
            fail(ready, w.id, w.fail.as_ref().unwrap_or(f));
        }
        for p in self.pre.drain(..) {
            fail(ready, p.id, f);
        }
    }

    /// Takes a request. It is written to `out` now, or after the login.
    pub fn send(&mut self, p: Pending, ready: &mut Vec<u8>) {
        if !matches!(self.state, State::Ready) {
            self.pre.push(p);
            return;
        }
        let Pending { id, job, at } = p;
        let r = match job {
            Job::Query(req) => self.encode(id, req, at, false),
            Job::Begin(key) => self.simple(id, "BEGIN", Kind::Begin(key), at),
            Job::End(true) => self.simple(id, "COMMIT", Kind::End, at),
            Job::End(false) | Job::Rollback => self.simple(id, "ROLLBACK", Kind::End, at),
        };
        if let Err(f) = r {
            fail(ready, id, &f);
        }
    }

    /// A simple query (no parameters, no rows): BEGIN, COMMIT, ROLLBACK.
    fn simple(&mut self, id: u64, sql: &str, kind: Kind, at: Instant) -> Result<(), Fail> {
        frontend::query(sql, &mut self.out).map_err(|e| Fail::text(&e.to_string()))?;
        self.waits.push_back(Wait {
            id,
            kind,
            at,
            cols: None,
            rows: Vec::new(),
            count: 0,
            fail: None,
            retry: false,
        });
        Ok(())
    }

    /// Encodes the query into `out`. `retry`: on the unnamed statement,
    /// with its own Parse.
    fn encode(&mut self, id: u64, req: Request, at: Instant, retry: bool) -> Result<(), Fail> {
        let Ok(sql) = std::str::from_utf8(&req.sql) else {
            return Err(Fail::text("dox_postgres_native: the sql is not UTF-8"));
        };
        let Conn {
            stmts,
            out,
            waits,
            names,
            tick,
            ..
        } = self;
        let k = key(&req);
        *tick += 1;
        let cached = !retry
            && match stmts.get_mut(&k) {
                Some(s)
                    if s.sql == req.sql
                        && s.oids.iter().copied().eq(req.params.iter().map(|p| p.oid)) =>
                {
                    s.used = *tick;
                    true
                }
                _ => false,
            };
        let evicted = (!retry && !cached)
            .then(|| evict(stmts, k, CACHE_MAX))
            .flatten();
        let new_name = (!retry && !cached).then(|| {
            *names += 1;
            format!("s{names}")
        });
        let name = match &new_name {
            Some(n) => n.as_str(),
            None if cached => stmts[&k].name.as_str(),
            None => "",
        };
        let start = out.len();
        let written = (|| -> Result<(), String> {
            let e = |e: std::io::Error| e.to_string();
            if let Some((_, old)) = &evicted {
                frontend::close(b'S', &old.name, out).map_err(e)?;
            }
            if !cached {
                frontend::parse(name, sql, req.params.iter().map(|p| p.oid), out).map_err(e)?;
                frontend::describe(b'S', name, out).map_err(e)?;
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
            frontend::execute("", 0, out).map_err(e)?;
            frontend::sync(out);
            Ok(())
        })();
        if let Err(e) = written {
            // nothing of this query goes out
            out.truncate(start);
            if let Some((old_key, old)) = evicted {
                stmts.insert(old_key, old);
            }
            return Err(Fail(2, e.into_bytes()));
        }
        if let Some(name) = new_name {
            stmts.insert(
                k,
                Stmt {
                    name,
                    sql: req.sql.clone(),
                    oids: req.params.iter().map(|p| p.oid).collect(),
                    cols: None,
                    used: *tick,
                },
            );
        }
        waits.push_back(Wait {
            id,
            kind: Kind::Query {
                stmt: (!retry).then_some(k),
                parsing: !cached,
                req: (!retry).then_some(req),
            },
            at,
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
    pub fn flush(&mut self, reg: &Registry, token: Token) -> Result<(), Fail> {
        if matches!(self.state, State::Connecting) {
            return Ok(());
        }
        let Conn { sock, tls, out, .. } = self;
        let blocked = match tls {
            None => loop {
                if out.is_empty() {
                    break false;
                }
                match sock.write(out) {
                    Ok(0) => return Err(write_error(ErrorKind::WriteZero)),
                    Ok(n) => out.advance(n),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break true,
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(e) => return Err(write_error(e)),
                }
            },
            Some(tls) => {
                if !out.is_empty() {
                    // no buffer limit: this takes all of it
                    tls.writer().write_all(out).map_err(write_error)?;
                    out.clear();
                }
                loop {
                    if !tls.wants_write() {
                        break false;
                    }
                    match tls.write_tls(sock) {
                        Ok(0) => return Err(write_error(ErrorKind::WriteZero)),
                        Ok(_) => {}
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break true,
                        Err(e) if e.kind() == ErrorKind::Interrupted => {}
                        Err(e) => return Err(write_error(e)),
                    }
                }
            }
        };
        if blocked != self.writable {
            self.writable = blocked;
            let i = self.interest();
            reg.reregister(&mut self.sock, token, i)
                .map_err(write_error)?;
        }
        Ok(())
    }

    /// Handles a poll event. `readable`: the socket may have data. Err:
    /// the connection is broken, its requests fail with it.
    pub fn event(
        &mut self,
        reg: &Registry,
        token: Token,
        readable: bool,
        ready: &mut Vec<u8>,
    ) -> Result<(), Fail> {
        if matches!(self.state, State::Connecting) {
            if !self.connected(reg, token)? {
                return Ok(());
            }
        } else if readable {
            self.read(ready)?;
        }
        if self.need_write() || self.writable {
            self.flush(reg, token)?;
        }
        Ok(())
    }

    /// Checks a connect in progress. Ok(true): it is done, the first
    /// message is in `out`.
    fn connected(&mut self, reg: &Registry, token: Token) -> Result<bool, Fail> {
        match self.sock.connected() {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(e) => return self.next_addr(reg, token, e),
        }
        match &self.sock {
            Sock::Tcp(s) => drop(s.set_nodelay(true)),
            #[cfg(unix)]
            Sock::Unix(_) => {}
        }
        // like libpq, no TLS on a Unix socket
        if self.pool.tls.is_some() && matches!(self.sock, Sock::Tcp(_)) {
            frontend::ssl_request(&mut self.out);
            self.state = State::SslReq;
        } else {
            self.startup()?;
        }
        Ok(true)
    }

    /// The connect to this address failed: try the next one.
    fn next_addr(
        &mut self,
        reg: &Registry,
        token: Token,
        mut e: std::io::Error,
    ) -> Result<bool, Fail> {
        let _ = reg.deregister(&mut self.sock);
        while self.next_addr < self.addrs.len() {
            let a = self.addrs[self.next_addr].clone();
            self.next_addr += 1;
            match Sock::dial(&a) {
                Ok(s) => {
                    self.sock = s;
                    self.writable = true;
                    self.register(reg, token).map_err(connect_error)?;
                    return Ok(false);
                }
                Err(e2) => e = e2,
            }
        }
        refresh(&self.pool);
        Err(connect_error(e))
    }

    /// Writes the startup message and waits for the login.
    fn startup(&mut self) -> Result<(), Fail> {
        let c = &self.pool.cfg;
        let mut params = vec![("client_encoding", "UTF8"), ("user", c.user.as_str())];
        if let Some(db) = &c.dbname {
            params.push(("database", db));
        }
        if let Some(o) = &c.options {
            params.push(("options", o));
        }
        if let Some(a) = &c.application_name {
            params.push(("application_name", a));
        }
        frontend::startup_message(params, &mut self.out).map_err(connect_error)?;
        self.state = State::Auth(None);
        Ok(())
    }

    /// The one byte answer to the SSLRequest.
    fn ssl_answer(&mut self) -> Result<(), Fail> {
        let answer = self.inb[0];
        // more bytes before the handshake: someone in the middle
        if self.filled > 1 {
            return Err(connect_error(
                "the server sent data before the TLS handshake",
            ));
        }
        self.filled = 0;
        let handshake = |e: String| Fail::text(&format!("error performing TLS handshake: {e}"));
        match answer {
            b'S' => {
                let config = self.pool.tls.clone().expect("TLS config");
                let name = rustls::pki_types::ServerName::try_from(self.pool.cfg.host.clone())
                    .map_err(|e| handshake(e.to_string()))?;
                let mut tls = rustls::ClientConnection::new(config, name)
                    .map_err(|e| handshake(e.to_string()))?;
                tls.set_buffer_limit(None);
                self.tls = Some(Box::new(tls));
            }
            b'N' if self.pool.cfg.ssl == Ssl::Require => {
                return Err(handshake("server does not support TLS".into()));
            }
            b'N' => {} // prefer: plain text
            _ => return Err(connect_error("bad answer to the SSLRequest")),
        }
        self.startup()
    }

    /// Makes room for one more read.
    fn room(inb: &mut Vec<u8>, filled: usize) {
        if inb.len() - filled < READ_ROOM {
            let size = (inb.len() * 2).max(READ_ROOM * 4);
            inb.resize(size, 0);
        }
    }

    /// Reads what the socket has into `inb`. Ok(true): the server closed
    /// the connection.
    fn fill(&mut self) -> Result<bool, Fail> {
        let Conn {
            sock,
            tls,
            inb,
            filled,
            ..
        } = self;
        let read_error =
            |e: std::io::Error| Fail::text(&format!("error communicating with the server: {e}"));
        let Some(tls) = tls else {
            loop {
                Self::room(inb, *filled);
                let room = inb.len() - *filled;
                match sock.read(&mut inb[*filled..]) {
                    Ok(0) => return Ok(true),
                    Ok(n) => {
                        *filled += n;
                        // A short read took all there was. More data later
                        // is a new edge (a new event), so no read just to
                        // see WouldBlock.
                        if n < room {
                            return Ok(false);
                        }
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(false),
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(e) => return Err(read_error(e)),
                }
            }
        };
        let mut eof = false;
        loop {
            let got = match tls.read_tls(sock) {
                Ok(0) => {
                    eof = true;
                    false
                }
                Ok(_) => true,
                Err(e) if e.kind() == ErrorKind::WouldBlock => false,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(read_error(e)),
            };
            let handshaking = tls.is_handshaking();
            if let Err(e) = tls.process_new_packets() {
                let what = if handshaking {
                    "error performing TLS handshake"
                } else {
                    "TLS error"
                };
                return Err(Fail::text(&format!("{what}: {e}")));
            }
            loop {
                Self::room(inb, *filled);
                match tls.reader().read(&mut inb[*filled..]) {
                    Ok(0) => {
                        eof = true; // close_notify
                        break;
                    }
                    Ok(n) => *filled += n,
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        eof = true;
                        break;
                    }
                    Err(e) => return Err(read_error(e)),
                }
            }
            if !got || eof {
                return Ok(eof);
            }
        }
    }

    /// Reads what the socket has, and adds the result records of the
    /// requests that are done to `ready`. Err: the connection is broken.
    pub fn read(&mut self, ready: &mut Vec<u8>) -> Result<(), Fail> {
        let eof = self.fill()?;
        if matches!(self.state, State::SslReq) && self.filled > 0 {
            self.ssl_answer()?;
        }
        self.messages(ready)?;
        if eof {
            return Err(Fail::text("connection closed"));
        }
        Ok(())
    }

    /// A connection that waited in the idle list of the pool: true when it
    /// still works.
    pub fn alive(&mut self) -> bool {
        let mut none = Vec::new();
        self.read(&mut none).is_ok()
    }

    /// Reads the whole messages in `inb`.
    fn messages(&mut self, ready: &mut Vec<u8>) -> Result<(), Fail> {
        let mut p = 0;
        // the login: a few messages, a copy of each is fine
        while matches!(self.state, State::Auth(_)) {
            let Some((tag, body)) = frame(&self.inb[..self.filled], p)? else {
                break;
            };
            let body = body.to_vec();
            p += 5 + body.len();
            self.login(tag, &body, ready)?;
        }
        let mut retries = Vec::new();
        if matches!(self.state, State::Ready) {
            p = self.replies(p, ready, &mut retries)?;
        }
        self.inb.copy_within(p..self.filled, 0);
        self.filled -= p;
        if self.filled == 0 && self.inb.len() > BIG_BUF {
            self.inb = Vec::new(); // do not keep a big buffer forever
        }
        for (id, req, at) in retries {
            if let Err(f) = self.encode(id, req, at, true) {
                fail(ready, id, &f);
            }
        }
        Ok(())
    }

    /// One message of the login.
    fn login(&mut self, tag: u8, body: &[u8], ready: &mut Vec<u8>) -> Result<(), Fail> {
        let enc = |e: std::io::Error| connect_error(e);
        match tag {
            b'R' => {
                let pool = self.pool.clone();
                let c = &pool.cfg;
                let data = &body[4..];
                match be32(body, 0) {
                    0 => {} // AuthenticationOk
                    3 => frontend::password_message(&c.password, &mut self.out).map_err(enc)?,
                    5 => {
                        let salt = data[..4].try_into().unwrap();
                        let hash = md5_hash(c.user.as_bytes(), &c.password, salt);
                        frontend::password_message(hash.as_bytes(), &mut self.out).map_err(enc)?;
                    }
                    10 => {
                        let scram = data.split(|&b| b == 0).any(|m| m == b"SCRAM-SHA-256");
                        if !scram {
                            return Err(connect_error(
                                "the server wants a SASL method that is not supported",
                            ));
                        }
                        let s = ScramSha256::new(&c.password, ChannelBinding::unsupported());
                        frontend::sasl_initial_response(
                            "SCRAM-SHA-256",
                            s.message(),
                            &mut self.out,
                        )
                        .map_err(enc)?;
                        self.state = State::Auth(Some(Box::new(s)));
                    }
                    n @ (11 | 12) => {
                        let State::Auth(Some(s)) = &mut self.state else {
                            return Err(connect_error("SASL out of order"));
                        };
                        if n == 11 {
                            s.update(data).map_err(enc)?;
                            frontend::sasl_response(s.message(), &mut self.out).map_err(enc)?;
                        } else {
                            s.finish(data).map_err(enc)?;
                        }
                    }
                    n => {
                        return Err(connect_error(format!("auth method {n} is not supported")));
                    }
                }
            }
            b'E' => return Err(Fail(1, error_body(body))),
            b'Z' => {
                self.state = State::Ready;
                for p in std::mem::take(&mut self.pre) {
                    self.send(p, ready);
                }
            }
            // ParameterStatus, BackendKeyData, Notice
            _ => {}
        }
        Ok(())
    }

    /// Reads the answers from `p` on, adds the result records to `ready`.
    /// Gives back where it stopped. The queries to send once more go to
    /// `retries`.
    fn replies(
        &mut self,
        mut p: usize,
        ready: &mut Vec<u8>,
        retries: &mut Vec<(u64, Request, Instant)>,
    ) -> Result<usize, Fail> {
        let Conn {
            inb,
            filled,
            stmts,
            waits,
            tx,
            last,
            ..
        } = self;
        while let Some((tag, body)) = frame(&inb[..*filled], p)? {
            p += 5 + body.len();
            match tag {
                // ParameterStatus, Notice, Notification: not for a request
                b'S' | b'N' | b'A' => continue,
                _ => {}
            }
            let Some(w) = waits.front_mut() else {
                // eg. a FATAL error when the server shuts down
                if tag == b'E' {
                    return Err(Fail(1, error_body(body)));
                }
                return Err(Fail::text("dox_postgres_native: an answer for no request"));
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
                    if let Kind::Query {
                        stmt: Some(k),
                        parsing: true,
                        ..
                    } = &w.kind
                        && let Some(s) = stmts.get_mut(k)
                    {
                        s.cols = Some(cols.clone());
                    }
                    w.cols = Some(cols);
                }
                b'E' => {
                    if let Kind::Query { stmt, parsing, req } = &w.kind {
                        let code = error_code(body);
                        let stale = code == GONE || code == STALE;
                        if let Some(k) = stmt
                            && (*parsing || stale)
                        {
                            // its Parse failed, or it is out of date: the
                            // next query prepares it again
                            stmts.remove(k);
                        }
                        w.retry = stale && !*parsing && req.is_some();
                    }
                    if w.fail.is_none() {
                        w.fail = Some(Fail(1, error_body(body)));
                    }
                }
                b'Z' => {
                    let w = waits.pop_front().unwrap();
                    if tx.is_some() {
                        *last = Instant::now();
                    }
                    if w.retry {
                        if let Kind::Query { req: Some(req), .. } = w.kind {
                            retries.push((w.id, req, w.at));
                        }
                        continue;
                    }
                    if let Some(f) = &w.fail {
                        // a failed BEGIN: no transaction on this connection
                        if let Kind::Begin(key) = w.kind
                            && *tx == Some(key)
                        {
                            *tx = None;
                        }
                        fail(ready, w.id, f);
                        continue;
                    }
                    match w.kind {
                        Kind::Query { stmt, .. } => {
                            let cols = w.cols.as_ref().or_else(|| {
                                stmt.and_then(|k| stmts.get(&k))
                                    .and_then(|s| s.cols.as_ref())
                            });
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
                        Kind::Begin(key) => record(ready, w.id, 3, &key.to_le_bytes()),
                        // no columns and no rows
                        Kind::End => record(ready, w.id, 0, &[0; 6]),
                    }
                }
                // ParseComplete, ParameterDescription, BindComplete,
                // CloseComplete, CommandComplete, EmptyQueryResponse,
                // PortalSuspended
                _ => {}
            }
        }
        Ok(p)
    }
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

    fn stmt(used: u64) -> Stmt {
        Stmt {
            name: format!("s{used}"),
            sql: Bytes::new(),
            oids: Vec::new(),
            cols: None,
            used,
        }
    }

    #[test]
    fn lru_closes_the_statement_used_longest_ago() {
        let mut stmts = HashMap::new();
        stmts.insert(1, stmt(5));
        stmts.insert(2, stmt(3));
        // room left: nothing to close
        assert!(evict(&mut stmts, 3, 3).is_none());
        // full: the one used longest ago (tick 3) goes
        let (k, s) = evict(&mut stmts, 3, 2).unwrap();
        assert_eq!((k, s.name.as_str()), (2, "s3"));
        // the same key (another sql, same hash) is closed first
        stmts.insert(2, stmt(9));
        let (k, _) = evict(&mut stmts, 2, 2).unwrap();
        assert_eq!(k, 2);
        assert_eq!(stmts.len(), 1);
    }
}
