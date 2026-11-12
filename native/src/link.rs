//! The link of one Dart isolate. All its Postgres work runs on the thread
//! of the isolate, like dox_http_native (its run.rs).
//!
//! A link has one mio `Poll` and the connections this isolate uses now.
//! - `dpg_submit` (leaf call) takes a batch of request records. A plain
//!   query goes to an idle connection of the link, or to a new one, or else
//!   to the one with the fewest waiting queries (pipelining). It is
//!   encoded and written to the socket at once. The bytes the socket does
//!   not take wait in the out buffer of the connection, and go when it is
//!   writable.
//! - `dpg_run` (leaf call) does one poll turn that does not wait. It reads
//!   the sockets that have data, runs the timers that are due, and writes
//!   the result records of the finished requests into Dart's buffer. It
//!   returns the byte count. It never waits.
//! - The idle helper (one small thread per link) watches the poll file
//!   (epoll or kqueue) while Dart is idle and the link has connections or
//!   waiting requests. It posts `[1]` to the Dart port when the poll file
//!   has an event, or when the next timer is due. It never takes the core
//!   for that: it only checks that the poll file is readable. The flag
//!   `armed` makes sure only one side tells Dart: the one that clears it.
//!   Every [PROBE] it also checks that the isolate is alive (an empty
//!   post). When it is gone, the link closes its connections and frees
//!   its poll file.
//! - Windows has no poll file to watch. There the helper takes the core
//!   for at most 1 ms at a time and polls itself, like dox_http_native.
//!
//! - `dpg_submit` and `dpg_run` are leaf calls, so they never call the
//!   Dart API. Only the helper posts to Dart. Code that must wake an
//!   isolate (its own or another) calls `nudge`: a flag, a kick of the
//!   helper and the mio `Waker` of its poll. The helper then posts `[1]`.
//!   `dpg_submit` returns 1 when results are ready at once; Dart then calls
//!   `dpg_run` by itself.
//!
//! A Dart isolate may move to another OS thread between two event loop
//! turns. So all state is `Send` and lives behind the mutex of the link.
//! Nothing uses thread locals.
//!
//! Transactions: `begin` takes an idle connection (or a new one) for
//! itself. Its queries go to that connection in order. After commit or
//! rollback the connection is a normal one again. A transaction with no
//! request for the request timeout of the pool (300 s by default) is
//! rolled back.
//!
//! A request that can be cancelled, and a stream, also take an idle
//! connection (or a new one) for themselves, so no other query is
//! pipelined behind them. A cancel (kind 5) drops a request that still
//! waits. For a running one, a small thread sends the CancelRequest on a
//! new socket (`send_cancel`), and nudges the link when the server closed
//! it. Free connections that are too old or idle too long (pool settings)
//! are closed by the timers.
//!
//! A transaction id is (link id << 32) | number. So a request for a
//! transaction from another isolate finds the link that owns it: it goes
//! to the `inbox` of that link, which sends it on the connection of the
//! transaction. The answer goes to the `outbox` of the link that asked.
//! The owner keeps the fast path: its own requests need no lookup. When
//! the owner isolate is gone, its transactions are rolled back, and such a
//! request gets "the transaction is closed".
//!
//! The connection limit of a pool is for the whole process: the pool
//! counts the connections of all links (`open`). When a link needs one
//! more and the pool is full, its requests wait in the link (`queue`), and
//! the link joins the `waiting` list of the pool. Then:
//! - The other links give their idle connections to the `idle` list of the
//!   pool (deregistered from their poll), and wake the links that wait. An
//!   idle link is asked by its helper (`share`).
//! - While another link waits, a link does not add new queries to its busy
//!   connections, and does not use its idle ones: they go to the pool. So
//!   connections drain and move on, and no link starves.
//! - A waiting link takes a connection from the pool `idle` list (or opens
//!   a new one when one closed) in the order of the `waiting` list. It
//!   registers it in its own poll and sends all its waiting requests.
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use mio::{Events, Poll, Registry, Token, Waker};

use crate::conn::{CancelJob, Conn, Job, Pending};
use crate::{
    Addr, DROPPED, Fail, INTERNAL, Pool, RES_HEAD, Request, fail, parse, pool, post, rd32, rd64,
    record, refresh,
};

/// How often the helper checks that the isolate is alive.
const PROBE: Duration = Duration::from_millis(100);
/// An empty result buffer bigger than this is made small again.
const BIG_BUF: usize = 1 << 20;

/// The token of the waker in the poll of each link.
const WAKER: Token = Token(usize::MAX);
/// Request ids with this bit came from another link (a transaction of this
/// link used in another isolate). The rest is a key in `Core::foreign`.
const FOREIGN: u64 = 1 << 62;

static NEXT_LINK: AtomicU64 = AtomicU64::new(1);
/// All links, to ask them for their idle connections.
static LINKS: Mutex<Vec<Weak<Link>>> = Mutex::new(Vec::new());

fn closed() -> Fail {
    Fail::text("the pool is closed")
}

fn tx_closed() -> Fail {
    Fail::text("the transaction is closed")
}

/// Sends a CancelRequest on a new socket, from a small thread: the
/// isolate never waits for it. Plain text, like libpq before Postgres 17:
/// the server takes it on any connection. When the server closed the
/// socket, the backend got the cancel: `done` is set and `link` is nudged,
/// so its connection takes requests again.
fn send_cancel(job: CancelJob, link: Weak<Link>) {
    let CancelJob {
        addr,
        pid,
        secret,
        wait,
        done,
    } = job;
    let mut msg = bytes::BytesMut::new();
    postgres_protocol::message::frontend::cancel_request(pid, secret, &mut msg);
    let run = move || {
        use std::io::{Read, Write};
        fn talk(mut s: impl Read + Write, msg: &[u8]) {
            if s.write_all(msg).is_ok() {
                // the server closes it when it is done
                let _ = s.read(&mut [0; 1]);
            }
        }
        match addr {
            Addr::Tcp(a) => {
                if let Ok(s) = std::net::TcpStream::connect_timeout(&a, wait) {
                    let _ = s.set_read_timeout(Some(wait));
                    let _ = s.set_write_timeout(Some(wait));
                    talk(s, &msg);
                }
            }
            #[cfg(unix)]
            Addr::Unix(p) => {
                if let Ok(s) = std::os::unix::net::UnixStream::connect(p) {
                    let _ = s.set_read_timeout(Some(wait));
                    let _ = s.set_write_timeout(Some(wait));
                    talk(s, &msg);
                }
            }
        }
        done.store(true, Ordering::Release);
        if let Some(l) = link.upgrade() {
            l.nudge();
        }
    };
    if let Err(e) = std::thread::Builder::new()
        .name("dox-pg-cancel".into())
        .spawn(run)
    {
        // no thread: the cancel is lost, the query runs to its end
        drop(e);
    }
}

/// A request of a transaction of this link, sent by another link.
struct Foreign {
    req: Request,
    /// the link that waits for the answer
    from: Weak<Link>,
}

/// The link with this id, while its isolate lives.
fn find(id: u64) -> Option<Arc<Link>> {
    let all = LINKS.lock().unwrap();
    all.iter()
        .filter_map(Weak::upgrade)
        .find(|l| l.id == id && !l.stopped.load(Ordering::Acquire))
}

/// Gives result records to another link: its next `dpg_run` hands them to
/// its isolate.
fn deliver(to: &Weak<Link>, records: &[u8]) {
    let Some(l) = to.upgrade() else { return };
    l.outbox.lock().unwrap().extend_from_slice(records);
    l.has_out.store(true, Ordering::Release);
    l.nudge();
}

/// A pool as one link sees it.
struct Local {
    pool: Arc<Pool>,
    /// tokens of the connections of this pool in the link
    conns: Vec<usize>,
    /// requests that wait for a connection
    queue: VecDeque<Pending>,
    /// the link is in the `waiting` list of the pool
    waiting: bool,
    /// in this call: the pool had no room for one more connection
    full: bool,
    /// in this call: the link got a connection from the pool
    grew: bool,
    /// in this call: the connection that got the last plain query
    pile: Option<usize>,
}

impl Local {
    /// Another link waits for a connection of this pool.
    fn others_wait(&self) -> bool {
        self.pool.wanted.load(Ordering::Relaxed) > usize::from(self.waiting)
    }
}

enum Grow {
    Conn(usize),
    Full,
    Fail(Fail),
}

struct Core {
    id: u64,
    /// the link of this core, for the `waiting` lists and the answers of
    /// other links
    me: Weak<Link>,
    poll: Poll,
    events: Events,
    /// by token
    conns: Vec<Option<Conn>>,
    /// by pool handle
    pools: HashMap<u64, Local>,
    /// result records not given to Dart yet
    ready: Vec<u8>,
    /// `ready` did not fit Dart's buffer: the next `dpg_run` only copies
    /// it, so it does not grow more in between
    held: bool,
    /// the next timer
    due: Option<Instant>,
    /// the next transaction number. A transaction id is (link id << 32) |
    /// number, so any link can find the link that owns it.
    next_tx: u64,
    /// requests from other links that wait for their answer here: key to
    /// (the link that asked, its request id)
    foreign: HashMap<u64, (Weak<Link>, u64)>,
    next_foreign: u64,
}

/// Puts `c` in a free slot of `conns` and in the poll. Gives back its token.
fn adopt(conns: &mut Vec<Option<Conn>>, reg: &Registry, mut c: Conn) -> Result<usize, Fail> {
    let i = conns
        .iter()
        .position(Option::is_none)
        .unwrap_or(conns.len());
    c.register(reg, Token(i))
        .map_err(|e| Fail::text(&format!("dox_postgres_native: {e}")))?;
    if i == conns.len() {
        conns.push(Some(c));
    } else {
        conns[i] = Some(c);
    }
    Ok(i)
}

/// One more connection for `l`: an idle one of the pool, or a new one.
/// Full when the pool has none for this link: the links that wait before
/// it get the free ones first.
fn grow(
    id: u64,
    l: &mut Local,
    conns: &mut Vec<Option<Conn>>,
    reg: &Registry,
    now: Instant,
) -> Grow {
    let pool = l.pool.clone();
    loop {
        let mut st = pool.state.lock().unwrap();
        let ahead = st
            .waiting
            .iter()
            .position(|w| w.0 == id)
            .unwrap_or(st.waiting.len());
        let spare = st.idle.len() + pool.max.saturating_sub(st.open);
        if spare <= ahead {
            return Grow::Full;
        }
        if let Some(mut c) = st.idle.pop() {
            drop(st); // a drop of a dead one takes the lock
            if !c.alive() || c.stale(now) {
                continue;
            }
            return match adopt(conns, reg, c) {
                Ok(i) => {
                    l.conns.push(i);
                    l.grew = true;
                    Grow::Conn(i)
                }
                Err(f) => Grow::Fail(f),
            };
        }
        st.open += 1;
        let addrs = match &st.addrs {
            Ok(a) => a.clone(),
            Err(f) => {
                // the last lookup failed: this request fails, a new
                // lookup runs on a thread
                let f = f.clone();
                st.open -= 1;
                drop(st);
                refresh(&pool);
                return Grow::Fail(f);
            }
        };
        drop(st);
        let c = match Conn::start(pool.clone(), addrs, now) {
            Ok(c) => c,
            Err(f) => {
                pool.lost();
                refresh(&pool);
                return Grow::Fail(f);
            }
        };
        return match adopt(conns, reg, c) {
            Ok(i) => {
                l.conns.push(i);
                l.grew = true;
                Grow::Conn(i)
            }
            Err(f) => Grow::Fail(f),
        };
    }
}

impl Core {
    fn new(id: u64, me: Weak<Link>, poll: Poll) -> Core {
        Core {
            id,
            me,
            poll,
            events: Events::with_capacity(256),
            conns: Vec::new(),
            pools: HashMap::new(),
            ready: Vec::new(),
            held: false,
            due: None,
            next_tx: 1,
            foreign: HashMap::new(),
            next_foreign: 0,
        }
    }

    /// The link owns connections, or requests wait: the helper must
    /// watch.
    fn pending(&self) -> bool {
        self.conns.iter().any(Option::is_some) || self.pools.values().any(|l| !l.queue.is_empty())
    }

    /// The pool `h` of this link. None when it is closed.
    fn local(&mut self, h: u64) -> Option<&mut Local> {
        if let std::collections::hash_map::Entry::Vacant(e) = self.pools.entry(h) {
            e.insert(Local {
                pool: pool(h)?,
                conns: Vec::new(),
                queue: VecDeque::new(),
                waiting: false,
                full: false,
                grew: false,
                pile: None,
            });
        }
        self.pools
            .get_mut(&h)
            .filter(|l| !l.pool.closed.load(Ordering::Acquire))
    }

    /// A new call: forget the notes of the last one.
    fn reset(&mut self) {
        for l in self.pools.values_mut() {
            l.full = false;
            l.grew = false;
            l.pile = None;
        }
    }

    /// Takes a batch of request records from Dart.
    fn submit(&mut self, b: &Bytes) {
        let now = Instant::now();
        self.reset();
        self.check(now);
        let mut p = 0;
        while let Some(req) = parse(b, &mut p) {
            match req.kind {
                5 => {
                    self.cancel(req.id);
                    continue;
                }
                6 => {
                    self.more(req.id, now);
                    continue;
                }
                _ if req.flags & DROPPED != 0 => {
                    record(&mut self.ready, req.id, 5, &[]);
                    continue;
                }
                _ => {}
            }
            if req.tx != 0 {
                self.in_tx(req, now);
                continue;
            }
            let (h, id) = (req.pool, req.id);
            let job = match req.kind {
                0 | 4 => Job::Query(req),
                1 => {
                    self.next_tx += 1;
                    Job::Begin(self.id << 32 | (self.next_tx & 0xffff_ffff))
                }
                _ => {
                    fail(&mut self.ready, id, &Fail::text("no transaction to end"));
                    continue;
                }
            };
            self.place(h, Pending { id, job, at: now });
        }
        self.after();
    }

    /// A request of a transaction: to its connection.
    fn in_tx(&mut self, req: Request, now: Instant) {
        let id = req.id;
        let found = self
            .conns
            .iter_mut()
            .flatten()
            .find(|c| c.tx == Some(req.tx));
        let Some(c) = found else {
            return self.forward(req);
        };
        let job = if req.kind == 0 || req.kind == 4 {
            Job::Query(req)
        } else {
            // new requests of this transaction fail from now on
            c.tx = None;
            Job::End(req.kind == 2)
        };
        c.send(Pending { id, job, at: now }, &mut self.ready);
    }

    /// A request of a transaction this link does not have: to the link of
    /// the isolate that started it. Its answer comes back with `deliver`.
    fn forward(&mut self, req: Request) {
        let id = req.id;
        let owner = req.tx >> 32;
        // ponytail: its next-chunk requests would need forwarding too
        if req.kind == 4 && owner != self.id {
            let f = Fail::text("a stream works only in the isolate that started the transaction");
            return fail(&mut self.ready, id, &f);
        }
        if owner != self.id
            && id & FOREIGN == 0
            && let Some(link) = find(owner)
        {
            let mut inbox = link.inbox.lock().unwrap();
            // checked under the lock: `gone` answers what is in the inbox
            if !link.stopped.load(Ordering::Acquire) {
                inbox.push(Foreign {
                    req,
                    from: self.me.clone(),
                });
                drop(inbox);
                link.has_in.store(true, Ordering::Release);
                link.nudge();
                return;
            }
        }
        fail(&mut self.ready, id, &tx_closed());
    }

    /// Takes the requests other links sent to the transactions of this
    /// link.
    fn take_foreign(&mut self, inbox: Vec<Foreign>, now: Instant) {
        for f in inbox {
            let key = self.next_foreign;
            self.next_foreign += 1;
            let mut req = f.req;
            self.foreign.insert(key, (f.from, req.id));
            req.id = FOREIGN | key;
            self.in_tx(req, now);
        }
    }

    /// Moves the answers to requests of other links from `ready` to those
    /// links.
    fn route(&mut self) {
        let Core { ready, foreign, .. } = self;
        let (mut keep, mut p) = (0, 0);
        while p + RES_HEAD <= ready.len() {
            let id = rd64(ready, p);
            let end = p + RES_HEAD + rd32(ready, p + 9) as usize;
            if id != INTERNAL
                && id & FOREIGN != 0
                && let Some((to, asked)) = foreign.remove(&(id & !FOREIGN))
            {
                let mut rec = ready[p..end].to_vec();
                rec[..8].copy_from_slice(&asked.to_le_bytes());
                deliver(&to, &rec);
            } else {
                ready.copy_within(p..end, keep);
                keep += end - p;
            }
            p = end;
        }
        ready.truncate(keep);
    }

    /// A plain query or a begin: to a connection, or it waits.
    fn place(&mut self, h: u64, p: Pending) {
        let Some(l) = self.local(h) else {
            return fail(&mut self.ready, p.id, &closed());
        };
        // keep the order: behind the ones that wait
        if !l.queue.is_empty() {
            return l.queue.push_back(p);
        }
        if let Err(p) = self.try_place(h, p) {
            self.pools.get_mut(&h).unwrap().queue.push_back(p);
        }
    }

    /// Gives `p` to a connection. Err: none for it now.
    fn try_place(&mut self, h: u64, p: Pending) -> Result<(), Pending> {
        let Core {
            id,
            poll,
            conns,
            pools,
            ready,
            ..
        } = self;
        let l = pools.get_mut(&h).unwrap();
        // a solo request needs a connection for itself, like a begin
        let begin = matches!(p.job, Job::Begin(_)) || p.solo();
        let others = l.others_wait();
        // 1. an idle connection of this link. Not while another link
        // waits: then it goes to the pool.
        let mut pick = if others {
            None
        } else {
            l.conns
                .iter()
                .copied()
                .find(|&i| conns[i].as_ref().is_some_and(Conn::idle))
        };
        // 2. one more connection
        if pick.is_none() && !l.full {
            match grow(*id, l, conns, poll.registry(), p.at) {
                Grow::Conn(i) => pick = Some(i),
                Grow::Full => l.full = true,
                Grow::Fail(f) => {
                    fail(ready, p.id, &f);
                    return Ok(());
                }
            }
        }
        // 3. a plain query: pipelined on the least busy connection. While
        // another link waits, only on the one this call already uses, so
        // the others drain and go to the pool.
        if pick.is_none() && !begin {
            pick = if others {
                l.pile
            } else {
                l.conns
                    .iter()
                    .copied()
                    .filter(|&i| {
                        conns[i]
                            .as_ref()
                            .is_some_and(|c| c.tx.is_none() && !c.exclusive())
                    })
                    .min_by_key(|&i| conns[i].as_ref().map_or(usize::MAX, Conn::load))
            };
        }
        let Some(i) = pick else { return Err(p) };
        let c = conns[i].as_mut().unwrap();
        match p.job {
            Job::Begin(key) => c.tx = Some(key),
            _ if begin => {}
            _ => l.pile = Some(i),
        }
        c.send(p, ready);
        Ok(())
    }

    /// Sends the requests that wait, while there are connections for them.
    fn pump(&mut self) {
        let mut todo: Vec<u64> = Vec::new();
        for (h, l) in &self.pools {
            if !l.queue.is_empty() && !l.pool.closed.load(Ordering::Acquire) {
                todo.push(*h);
            }
        }
        for h in todo {
            while let Some(p) = self.pools.get_mut(&h).unwrap().queue.pop_front() {
                if let Err(p) = self.try_place(h, p) {
                    self.pools.get_mut(&h).unwrap().queue.push_front(p);
                    break;
                }
            }
        }
    }

    /// Joins or leaves the `waiting` list of each pool. A link that got a
    /// connection and still waits goes to the end of the list.
    fn wait_turns(&mut self) {
        let Core { id, me, pools, .. } = self;
        let id = *id;
        let mut ask = false;
        for l in pools.values_mut() {
            let need = !l.queue.is_empty();
            if need == l.waiting && !(need && l.grew) {
                continue;
            }
            let mut st = l.pool.state.lock().unwrap();
            st.waiting.retain(|w| w.0 != id);
            if need {
                st.waiting.push_back((id, me.clone()));
                // a connection came free before this link joined: try
                // again at once, nobody else wakes it for that one
                let spare = st.idle.len() + l.pool.max.saturating_sub(st.open);
                if spare >= st.waiting.len()
                    && let Some(me) = me.upgrade()
                {
                    me.nudge();
                }
            }
            l.pool.wanted.store(st.waiting.len(), Ordering::Relaxed);
            ask |= need && !l.waiting;
            l.waiting = need;
        }
        if ask {
            share_all(id);
        }
    }

    /// Gives idle connections to the pool when another link waits, and
    /// closes the connections of closed pools.
    fn share(&mut self) {
        let Core {
            id,
            poll,
            conns,
            pools,
            ready,
            ..
        } = self;
        pools.retain(|_, l| {
            let shut = l.pool.closed.load(Ordering::Acquire);
            if !shut && !l.others_wait() {
                return true;
            }
            let mut give = Vec::new();
            l.conns.retain(|&i| {
                let c = conns[i].as_ref().unwrap();
                // a closed pool: also a connection that is not logged in
                // yet, when nothing waits on it
                let free = c.idle() || (shut && c.load() == 0 && c.tx.is_none());
                if free {
                    let mut c = conns[i].take().unwrap();
                    c.deregister(poll.registry());
                    give.push(c);
                }
                !free
            });
            if shut {
                for p in l.queue.drain(..) {
                    fail(ready, p.id, &closed());
                }
                drop(give); // closes them
                if l.waiting {
                    let mut st = l.pool.state.lock().unwrap();
                    st.waiting.retain(|w| w.0 != *id);
                    l.pool.wanted.store(st.waiting.len(), Ordering::Relaxed);
                    l.waiting = false;
                }
                // keep it while its connections finish their requests
                return !l.conns.is_empty();
            }
            if !give.is_empty() {
                let mut st = l.pool.state.lock().unwrap();
                st.idle.append(&mut give);
                Pool::wake(&st);
            }
            true
        });
    }

    /// Writes the out buffers that have bytes.
    fn flush_all(&mut self) {
        let mut broken = Vec::new();
        for (i, c) in self.conns.iter_mut().enumerate() {
            if let Some(c) = c
                && let Err(f) = c.flush(self.poll.registry(), Token(i))
            {
                broken.push((i, f));
            }
        }
        for (i, f) in broken {
            self.kill(i, &f);
        }
    }

    /// Closes connection `i`. Its requests fail with `f`.
    fn kill(&mut self, i: usize, f: &Fail) {
        let Some(mut c) = self.conns[i].take() else {
            return;
        };
        c.deregister(self.poll.registry());
        c.fail_all(f, &mut self.ready);
        for l in self.pools.values_mut() {
            l.conns.retain(|&x| x != i);
        }
    }

    /// Reads the events that are there, waiting up to `wait` for one.
    fn turn(&mut self, wait: Duration) {
        if self.poll.poll(&mut self.events, Some(wait)).is_err() {
            return;
        }
        let mut broken = Vec::new();
        for ev in self.events.iter() {
            let i = ev.token().0;
            let Some(c) = self.conns.get_mut(i).and_then(Option::as_mut) else {
                continue;
            };
            let closed = ev.is_read_closed() || ev.is_error();
            let readable = ev.is_readable() || closed;
            let ready = &mut self.ready;
            if let Err(f) = c.event(self.poll.registry(), Token(i), readable, closed, ready) {
                broken.push((i, f));
            }
        }
        for (i, f) in broken {
            self.kill(i, &f);
        }
    }

    /// Runs the timers when one is due.
    fn check(&mut self, now: Instant) {
        if self.due.is_none_or(|d| d > now) {
            return;
        }
        let mut broken = Vec::new();
        for (i, c) in self.conns.iter_mut().enumerate() {
            if let Some(c) = c
                && let Err(f) = c.expire(now, &mut self.ready)
            {
                broken.push((i, f));
            } else if let Some(c) = c
                && c.idle()
                && c.stale(now)
            {
                // too old or idle too long: closed, nothing waits on it
                broken.push((i, Fail::text("")));
            }
        }
        for (i, f) in broken {
            self.kill(i, &f);
        }
        for l in self.pools.values_mut() {
            let to = l.pool.timeout();
            while l.queue.front().is_some_and(|p| p.at + to <= now) {
                let p = l.queue.pop_front().unwrap();
                fail(&mut self.ready, p.id, &l.pool.timed_out());
            }
        }
    }

    /// Cancels request `id`: one that waits for a connection is dropped,
    /// one on a connection is cancelled there. Not found (eg. it has its
    /// answer already): nothing to do.
    fn cancel(&mut self, id: u64) {
        for l in self.pools.values_mut() {
            if let Some(i) = l.queue.iter().position(|p| p.id == id) {
                l.queue.remove(i);
                return record(&mut self.ready, id, 5, &[]);
            }
        }
        for c in self.conns.iter_mut().flatten() {
            if c.cancel(id, &mut self.ready) {
                return;
            }
        }
    }

    /// Dart wants the next chunk of stream `id`.
    fn more(&mut self, id: u64, now: Instant) {
        for c in self.conns.iter_mut().flatten() {
            if c.more(id, now) {
                return;
            }
        }
    }

    /// Sends the CancelRequests that are due.
    fn cancels(&mut self) {
        let mut now = None;
        for c in self.conns.iter_mut().flatten() {
            if !c.cancelling() {
                continue;
            }
            let now = *now.get_or_insert_with(Instant::now);
            if let Some(job) = c.cancel_job(now) {
                send_cancel(job, self.me.clone());
            }
        }
    }

    /// The time of the next timer.
    fn next_due(&self) -> Option<Instant> {
        let conns = self.conns.iter().flatten().filter_map(Conn::due);
        let queues = self
            .pools
            .values()
            .filter_map(|l| l.queue.front().map(|p| p.at + l.pool.timeout()));
        conns.chain(queues).min()
    }

    /// The end of each call: give or take connections, write, set the
    /// next timer.
    fn after(&mut self) {
        if !self.foreign.is_empty() {
            self.route();
        }
        self.share();
        self.wait_turns();
        self.flush_all();
        // after the flush: the query is on its way before its cancel
        self.cancels();
        self.due = self.next_due();
    }

    /// One turn for Dart: see `dpg_run`.
    fn run(&mut self, inbox: Vec<Foreign>) {
        self.reset();
        self.turn(Duration::ZERO);
        let now = Instant::now();
        self.check(now);
        self.take_foreign(inbox, now);
        self.pump();
        self.after();
    }

    /// The isolate is gone: leave the `waiting` lists. Dropping the core
    /// then closes the connections.
    fn leave(&mut self) {
        for l in self.pools.values_mut() {
            if l.waiting {
                let mut st = l.pool.state.lock().unwrap();
                st.waiting.retain(|w| w.0 != self.id);
                l.pool.wanted.store(st.waiting.len(), Ordering::Relaxed);
                Pool::wake(&st);
            }
        }
    }
}

/// Asks every link but `except` to give its idle connections to the pools
/// where another link waits, and to close the connections of closed
/// pools.
pub(crate) fn share_all(except: u64) {
    let links: Vec<Arc<Link>> = {
        let mut all = LINKS.lock().unwrap();
        all.retain(|w| w.strong_count() > 0);
        all.iter().filter_map(Weak::upgrade).collect()
    };
    for l in links {
        if l.id != except {
            l.share.store(true, Ordering::Release);
            l.kick();
        }
    }
}

/// The link of one isolate. Dart holds an `Arc` of it as handle.
pub(crate) struct Link {
    id: u64,
    /// None after the isolate is gone
    core: Mutex<Option<Core>>,
    /// the poll file (epoll or kqueue), watched by the idle helper
    #[cfg(unix)]
    fd: i32,
    /// Dart is idle: the helper watches, and tells Dart when work comes
    armed: AtomicBool,
    /// the next timer of the core, in nanoseconds after `base`. `u64::MAX`:
    /// none.
    due: AtomicU64,
    base: Instant,
    /// wakes the helper: Dart went idle, or another link asks to share
    kick: Mutex<bool>,
    cv: Condvar,
    stopped: AtomicBool,
    /// another link waits: give the idle connections to the pool
    share: AtomicBool,
    /// Dart must run: the helper posts `[1]`. Only the helper posts, never
    /// a leaf call from Dart.
    wake: AtomicBool,
    /// ends a wait of the helper on the poll file
    waker: Waker,
    /// requests of other links for the transactions of this link
    inbox: Mutex<Vec<Foreign>>,
    has_in: AtomicBool,
    /// answers from other links for this isolate
    outbox: Mutex<Vec<u8>>,
    has_out: AtomicBool,
    port: i64,
}

impl Link {
    fn kick(&self) {
        *self.kick.lock().unwrap() = true;
        self.cv.notify_one();
    }

    /// Makes the helper post `[1]` to Dart soon, so Dart calls `dpg_run`.
    /// Safe from any thread and from a leaf call: no Dart API here.
    pub(crate) fn nudge(&self) {
        self.wake.store(true, Ordering::Release);
        self.kick();
        let _ = self.waker.wake();
    }

    /// The helper: tells Dart to run when someone asked for it. False when
    /// the isolate is gone.
    fn wake_now(&self) -> bool {
        !self.wake.swap(false, Ordering::AcqRel) || post(self.port, &[1])
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
        self.kick();
    }

    fn set_due(&self, at: Option<Instant>) {
        let ns = at.map_or(u64::MAX, |t| {
            t.saturating_duration_since(self.base)
                .as_nanos()
                .min(u128::from(u64::MAX - 1)) as u64
        });
        self.due.store(ns, Ordering::Release);
    }

    /// How long until the next timer of the core. None: no timer.
    fn due_in(&self) -> Option<Duration> {
        let ns = self.due.load(Ordering::Acquire);
        (ns != u64::MAX).then(|| {
            (self.base + Duration::from_nanos(ns)).saturating_duration_since(Instant::now())
        })
    }

    /// true when the poll file has an event within `wait`. Does not read
    /// the event and does not take the core: `dpg_run` does that.
    #[cfg(unix)]
    fn ready_within(&self, wait: Duration) -> bool {
        let mut p = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // round up, so a timer a bit in the future does not spin
        let ms = wait.as_micros().div_ceil(1000).min(i32::MAX as u128) as i32;
        let r = unsafe { libc::poll(&mut p, 1, ms) };
        r > 0 && p.revents & libc::POLLIN != 0
    }

    /// Windows has no poll file to watch: take the core for at most 1 ms
    /// at a time, so Dart waits at most that long.
    #[cfg(not(unix))]
    fn ready_within(&self, wait: Duration) -> bool {
        let mut core = self.core.lock().unwrap();
        let Some(core) = core.as_mut() else {
            return false;
        };
        core.turn(wait.min(Duration::from_millis(1)));
        !core.ready.is_empty()
    }

    /// Another link asked for idle connections: give them now, also when
    /// Dart is idle.
    fn share_now(&self) {
        if !self.share.swap(false, Ordering::AcqRel) {
            return;
        }
        let mut core = self.core.lock().unwrap();
        let Some(c) = core.as_mut() else { return };
        c.share();
        c.wait_turns();
        let has_ready = !c.ready.is_empty();
        c.due = c.next_due();
        self.set_due(c.due);
        drop(core);
        // the helper may post: it is not a leaf call
        if has_ready {
            post(self.port, &[1]);
        }
    }

    /// The isolate is gone: close its connections and free the poll file.
    fn gone(&self) {
        self.stopped.store(true, Ordering::Release);
        let closed = |id: u64| {
            let mut r = Vec::new();
            fail(&mut r, id, &tx_closed());
            r
        };
        // its transactions roll back when their connections close
        for f in std::mem::take(&mut *self.inbox.lock().unwrap()) {
            deliver(&f.from, &closed(f.req.id));
        }
        let core = self.core.lock().unwrap().take();
        if let Some(mut core) = core {
            core.leave();
            for (to, asked) in core.foreign.values() {
                deliver(to, &closed(*asked));
            }
            drop(core);
        }
        LINKS
            .lock()
            .unwrap()
            .retain(|w| !std::ptr::eq(w.as_ptr(), self));
        // Dart never uses its handle again: give back its reference. The
        // helper holds one more until it returns.
        unsafe { Arc::decrement_strong_count(self as *const Link) };
    }

    /// The idle helper: watches the poll file while Dart is idle. Also
    /// checks every [PROBE] that the isolate is alive, idle or not.
    fn helper(&self) {
        let mut probed = Instant::now();
        loop {
            {
                let mut k = self.kick.lock().unwrap();
                while !*k {
                    k = self.cv.wait_timeout(k, PROBE).unwrap().0;
                    if !*k && probed.elapsed() >= PROBE {
                        // A dead isolate never calls `dpg_run` again, so
                        // check it here too.
                        probed = Instant::now();
                        if !post(self.port, &[]) {
                            drop(k);
                            return self.gone();
                        }
                    }
                }
                *k = false;
            }
            self.share_now();
            if !self.wake_now() {
                return self.gone();
            }
            while self.armed.load(Ordering::Acquire) {
                let wait = self.due_in().map_or(PROBE, |d| d.min(PROBE));
                let ready = self.ready_within(wait);
                self.share_now();
                if !self.wake_now() {
                    return self.gone();
                }
                if ready || self.due_in() == Some(Duration::ZERO) {
                    // only the side that clears `armed` tells Dart
                    if self
                        .armed
                        .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                        && !post(self.port, &[1])
                    {
                        return self.gone();
                    }
                    break;
                }
                if probed.elapsed() >= PROBE {
                    probed = Instant::now();
                    if !post(self.port, &[]) {
                        return self.gone();
                    }
                }
            }
        }
    }
}

// ---- FFI ---------------------------------------------------------------

/// Opens the link of one isolate. Its idle helper posts to `send_port`.
/// Returns a handle (> 0), or 0 when it can not (no poll or no thread).
#[unsafe(no_mangle)]
pub extern "C" fn dpg_open(send_port: i64) -> i64 {
    let Ok(poll) = Poll::new() else { return 0 };
    #[cfg(unix)]
    let fd = {
        use std::os::fd::AsRawFd;
        poll.as_raw_fd()
    };
    let Ok(waker) = Waker::new(poll.registry(), WAKER) else {
        return 0;
    };
    let id = NEXT_LINK.fetch_add(1, Ordering::Relaxed);
    let link = Arc::new_cyclic(|me| Link {
        id,
        core: Mutex::new(Some(Core::new(id, me.clone(), poll))),
        #[cfg(unix)]
        fd,
        armed: AtomicBool::new(false),
        due: AtomicU64::new(u64::MAX),
        base: Instant::now(),
        kick: Mutex::new(false),
        cv: Condvar::new(),
        stopped: AtomicBool::new(false),
        share: AtomicBool::new(false),
        wake: AtomicBool::new(false),
        waker,
        inbox: Mutex::new(Vec::new()),
        has_in: AtomicBool::new(false),
        outbox: Mutex::new(Vec::new()),
        has_out: AtomicBool::new(false),
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
    LINKS.lock().unwrap().push(Arc::downgrade(&link));
    Arc::into_raw(link) as i64
}

/// Takes a batch of request records. Each request goes to a connection of
/// this link (or waits for one), and is written to its socket at once.
/// Results come with `dpg_run`. Returns 1 when some are ready at once
/// (eg. an error): then Dart calls `dpg_run` soon. Else 0.
///
/// A leaf call: it never calls the Dart API (no post).
///
/// # Safety
/// `link` must come from `dpg_open`. `ptr` must point to `len` readable
/// bytes. Only one call per link at a time (one isolate).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_submit(link: i64, ptr: *const u8, len: i64) -> i64 {
    let s = unsafe { &*(link as *const Link) };
    if s.stopped.load(Ordering::Acquire) {
        return 0;
    }
    let b = unsafe { crate::bytes(ptr, len) };
    // one copy of the batch: Dart reuses its buffer after this call.
    // Requests keep slices of it (for a retry, or until a connection is
    // ready).
    let b = Bytes::copy_from_slice(b);
    let mut core = s.core.lock().unwrap();
    let Some(c) = core.as_mut() else { return 0 };
    c.submit(&b);
    let has_ready = !c.ready.is_empty();
    let pending = c.pending();
    s.set_due(c.due);
    drop(core);
    if pending && !s.armed.load(Ordering::Acquire) {
        s.arm();
    }
    i64::from(has_ready)
}

/// Reads the sockets of this link and writes the result records of the
/// finished requests to `buf`. Never waits. Returns the byte count (0:
/// nothing yet, the idle helper tells Dart when an answer comes), or -n:
/// the results need a buffer of at least n bytes (call again with a
/// bigger one).
///
/// A leaf call: it never calls the Dart API (no post). Other links are
/// woken through their helper (`nudge`).
///
/// # Safety
/// `link` must come from `dpg_open`. `buf` must point to `cap` writable
/// bytes. Only one call per link at a time (one isolate).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_run(link: i64, buf: *mut u8, cap: i64) -> i64 {
    let s = unsafe { &*(link as *const Link) };
    let mut core = s.core.lock().unwrap();
    let Some(c) = core.as_mut() else { return 0 };
    s.armed.store(false, Ordering::Release);
    if !c.held {
        let inbox = if s.has_in.swap(false, Ordering::AcqRel) {
            std::mem::take(&mut *s.inbox.lock().unwrap())
        } else {
            Vec::new()
        };
        c.run(inbox);
        if s.has_out.swap(false, Ordering::AcqRel) {
            let out = std::mem::take(&mut *s.outbox.lock().unwrap());
            c.ready.extend_from_slice(&out);
        }
    }
    let n = c.ready.len();
    if n > cap.max(0) as usize {
        // Dart calls again at once with a buffer this big
        c.held = true;
        return -(n as i64);
    }
    c.held = false;
    unsafe { std::ptr::copy_nonoverlapping(c.ready.as_ptr(), buf, n) };
    c.ready.clear();
    if c.ready.capacity() > BIG_BUF {
        c.ready = Vec::new(); // do not keep a big buffer forever
    }
    let pending = c.pending();
    s.set_due(c.due);
    drop(core);
    if pending {
        s.arm();
    }
    n as i64
}
