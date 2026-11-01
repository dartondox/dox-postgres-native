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
//! A Dart isolate may move to another OS thread between two event loop
//! turns. So all state is `Send` and lives behind the mutex of the link.
//! Nothing uses thread locals.
//!
//! Transactions: `begin` takes an idle connection (or a new one) for
//! itself. Its queries go to that connection in order. After commit or
//! rollback the connection is a normal one again. A transaction with no
//! request for the timeout (300 s) is rolled back.
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
use mio::{Events, Poll, Registry, Token};

use crate::conn::{Conn, Job, Pending};
use crate::{Fail, Pool, Request, TIMED_OUT, fail, parse, pool, post, refresh};

/// How often the helper checks that the isolate is alive.
const PROBE: Duration = Duration::from_millis(100);
/// An empty result buffer bigger than this is made small again.
const BIG_BUF: usize = 1 << 20;

static NEXT_LINK: AtomicU64 = AtomicU64::new(1);
static NEXT_TX: AtomicU64 = AtomicU64::new(1);
/// All links, to ask them for their idle connections.
static LINKS: Mutex<Vec<Weak<Link>>> = Mutex::new(Vec::new());

fn closed() -> Fail {
    Fail::text("the pool is closed")
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
    port: i64,
    poll: Poll,
    events: Events,
    /// by token
    conns: Vec<Option<Conn>>,
    /// by pool handle
    pools: HashMap<u64, Local>,
    /// result records not given to Dart yet
    ready: Vec<u8>,
    /// the next timer
    due: Option<Instant>,
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
            if !c.alive() {
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
    fn new(id: u64, port: i64, poll: Poll) -> Core {
        Core {
            id,
            port,
            poll,
            events: Events::with_capacity(256),
            conns: Vec::new(),
            pools: HashMap::new(),
            ready: Vec::new(),
            due: None,
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
            if req.tx != 0 {
                self.in_tx(req, now);
                continue;
            }
            let (h, id) = (req.pool, req.id);
            let job = match req.kind {
                0 => Job::Query(req),
                1 => Job::Begin(NEXT_TX.fetch_add(1, Ordering::Relaxed)),
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
            return fail(
                &mut self.ready,
                id,
                &Fail::text("the transaction is closed"),
            );
        };
        let job = if req.kind == 0 {
            Job::Query(req)
        } else {
            // new requests of this transaction fail from now on
            c.tx = None;
            Job::End(req.kind == 2)
        };
        c.send(Pending { id, job, at: now }, &mut self.ready);
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
        let begin = matches!(p.job, Job::Begin(_));
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
                    .filter(|&i| conns[i].as_ref().is_some_and(|c| c.tx.is_none()))
                    .min_by_key(|&i| conns[i].as_ref().map_or(usize::MAX, Conn::load))
            };
        }
        let Some(i) = pick else { return Err(p) };
        let c = conns[i].as_mut().unwrap();
        match p.job {
            Job::Begin(key) => c.tx = Some(key),
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
        let (id, port) = (self.id, self.port);
        let mut ask = false;
        for l in self.pools.values_mut() {
            let need = !l.queue.is_empty();
            if need == l.waiting && !(need && l.grew) {
                continue;
            }
            let mut st = l.pool.state.lock().unwrap();
            st.waiting.retain(|w| w.0 != id);
            if need {
                st.waiting.push_back((id, port));
                // a connection came free before this link joined: try
                // again at once, nobody else wakes it for that one
                let spare = st.idle.len() + l.pool.max.saturating_sub(st.open);
                if spare >= st.waiting.len() {
                    post(port, &[1]);
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
            let readable = ev.is_readable() || ev.is_read_closed() || ev.is_error();
            if let Err(f) = c.event(self.poll.registry(), Token(i), readable, &mut self.ready) {
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
            }
        }
        for (i, f) in broken {
            self.kill(i, &f);
        }
        for l in self.pools.values_mut() {
            let to = l.pool.timeout();
            while l.queue.front().is_some_and(|p| p.at + to <= now) {
                let p = l.queue.pop_front().unwrap();
                fail(&mut self.ready, p.id, &Fail::text(TIMED_OUT));
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
        self.share();
        self.wait_turns();
        self.flush_all();
        self.due = self.next_due();
    }

    /// One turn for Dart: see `dpg_run`.
    fn run(&mut self) {
        self.reset();
        self.turn(Duration::ZERO);
        self.check(Instant::now());
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
struct Link {
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
    port: i64,
}

impl Link {
    fn kick(&self) {
        *self.kick.lock().unwrap() = true;
        self.cv.notify_one();
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
        if has_ready {
            post(self.port, &[1]);
        }
    }

    /// The isolate is gone: close its connections and free the poll file.
    fn gone(&self) {
        self.stopped.store(true, Ordering::Release);
        let core = self.core.lock().unwrap().take();
        if let Some(mut core) = core {
            core.leave();
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
            while self.armed.load(Ordering::Acquire) {
                let wait = self.due_in().map_or(PROBE, |d| d.min(PROBE));
                let ready = self.ready_within(wait);
                self.share_now();
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
    let id = NEXT_LINK.fetch_add(1, Ordering::Relaxed);
    let link = Arc::new(Link {
        id,
        core: Mutex::new(Some(Core::new(id, send_port, poll))),
        #[cfg(unix)]
        fd,
        armed: AtomicBool::new(false),
        due: AtomicU64::new(u64::MAX),
        base: Instant::now(),
        kick: Mutex::new(false),
        cv: Condvar::new(),
        stopped: AtomicBool::new(false),
        share: AtomicBool::new(false),
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
/// Results come with `dpg_run`. When some are ready at once (eg. an
/// error), Dart gets `[1]` on its port.
///
/// # Safety
/// `link` must come from `dpg_open`. `ptr` must point to `len` readable
/// bytes. Only one call per link at a time (one isolate).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dpg_submit(link: i64, ptr: *const u8, len: i64) {
    let s = unsafe { &*(link as *const Link) };
    if s.stopped.load(Ordering::Acquire) {
        return;
    }
    let b = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
    // one copy of the batch: Dart reuses its buffer after this call.
    // Requests keep slices of it (for a retry, or until a connection is
    // ready).
    let b = Bytes::copy_from_slice(b);
    let mut core = s.core.lock().unwrap();
    let Some(c) = core.as_mut() else { return };
    c.submit(&b);
    let has_ready = !c.ready.is_empty();
    let pending = c.pending();
    s.set_due(c.due);
    drop(core);
    if has_ready {
        // Dart reads it with its next run
        post(s.port, &[1]);
    } else if pending && !s.armed.load(Ordering::Acquire) {
        s.arm();
    }
}

/// Reads the sockets of this link and writes the result records of the
/// finished requests to `buf`. Never waits. Returns the byte count (0:
/// nothing yet, the idle helper tells Dart when an answer comes), or -n:
/// the results need a buffer of at least n bytes (call again with a
/// bigger one).
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
    c.run();
    let n = c.ready.len();
    if n > cap.max(0) as usize {
        return -(n as i64);
    }
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
