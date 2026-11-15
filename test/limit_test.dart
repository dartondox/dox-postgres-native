import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'dart:math';

import 'package:dox_postgres_native/dox_postgres_native.dart';
import 'package:dox_postgres_native/src/bindings.dart';
import 'package:test/test.dart';

// Postgres like in CI. DB_PORT=5472 dart test, for another port.
String host = Platform.environment['DB_HOST'] ?? 'localhost';
int port = int.parse(Platform.environment['DB_PORT'] ?? '5432');

/// A pool of its own: the application name makes the settings unique, and
/// lets [count] find its connections.
PgNativePool openPool(String app, int max) => PgNativePool.open(
  host: host,
  port: port,
  user: 'postgres',
  password: 'postgres',
  database: 'postgres',
  maxConnections: max,
  applicationName: app,
);

late PgNativePool watcher;

/// The connections of [app] the server has now.
Future<int> count(String app) async {
  PgNativeResult r = await watcher.query(
    'SELECT count(*) AS n FROM pg_stat_activity WHERE application_name = @a',
    parameters: <String, String>{'a': app},
  );
  return r.rows.single['n'] as int;
}

/// Waits until [app] has [n] connections, for up to 5 s.
Future<void> settle(String app, int n) async {
  for (int i = 0; i < 100; i++) {
    if (await count(app) == n) return;
    await Future<void>.delayed(const Duration(milliseconds: 50));
  }
  expect(await count(app), n);
}

void main() {
  setUpAll(() => watcher = openPool('dpg_watcher', 2));
  tearDownAll(() => watcher.close());

  test('maxConnections is for the whole process', () async {
    String app = 'dpg_limit_$pid';
    PgNativePool pool = openPool(app, 3);
    int most = 0;
    bool running = true;
    Future<void> watch = () async {
      while (running) {
        int n = await count(app);
        if (n > most) most = n;
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
    }();
    int handle = pool.handle;
    List<int> sums = await Future.wait(<Future<int>>[
      for (int w = 0; w < 4; w++) Isolate.run(_sleepy(handle, w)),
    ]).timeout(const Duration(seconds: 30));
    running = false;
    await watch;
    for (int w = 0; w < 4; w++) {
      expect(sums[w], 100 * 100 * w + 99 * 100 ~/ 2);
    }
    expect(most, inInclusiveRange(1, 3));
    pool.close();
    await settle(app, 0);
  });

  test('two busy isolates each keep their share of a full pool', () async {
    String app = 'dpg_fair_$pid';
    PgNativePool pool = openPool(app, 4);
    int handle = pool.handle;
    // the first one takes the whole pool, then the second one comes
    List<List<List<int>>> runs = await Future.wait(<Future<List<List<int>>>>[
      for (int w = 0; w < 2; w++) Isolate.run(_spread(handle, w * 300)),
    ]).timeout(const Duration(seconds: 30));
    for (List<List<int>> r in runs) {
      // once both are busy (from 700 ms), each round spreads on about its
      // share (4 / 2 = 2), and does not pile all its queries on one
      List<int> used = <int>[
        for (List<int> round in r)
          if (round[0] >= 700) round.skip(1).toSet().length,
      ];
      expect(used, isNotEmpty);
      int spread = used.where((int n) => n >= 2).length;
      expect(
        spread,
        greaterThanOrEqualTo(used.length * 3 ~/ 4),
        reason: '$used',
      );
    }
    expect(await count(app), lessThanOrEqualTo(4));
    pool.close();
    await settle(app, 0);
  });

  test('idle connections of another isolate go over the share', () async {
    String app = 'dpg_spare_$pid';
    PgNativePool pool = openPool(app, 8);
    ReceivePort fromChild = ReceivePort();
    StreamIterator<Object?> msgs = StreamIterator<Object?>(fromChild);
    await Isolate.spawn(_idleThenBusy, <Object>[
      pool.handle,
      fromChild.sendPort,
    ]);
    expect(await msgs.moveNext(), isTrue);
    SendPort toChild = msgs.current as SendPort;
    // the other isolate holds 4 connections and is idle now: the share
    // of each isolate is 8 / 2 = 4
    expect(await msgs.moveNext(), isTrue);
    expect(msgs.current, 'holding');
    // plain queries: this isolate gets more than its share
    int most = 0;
    for (int i = 0; i < 40 && most <= 4; i++) {
      most = max(most, await _round(pool, 16));
    }
    expect(most, greaterThan(4));
    // 6 open transactions at once: more than the share
    List<int> txs = await Future.wait(<Future<int>>[
      for (int i = 0; i < 6; i++) pool.begin(),
    ]).timeout(const Duration(seconds: 5));
    for (int tx in txs) {
      await pool.query('SELECT 1', transaction: tx);
    }
    for (int tx in txs) {
      await pool.commit(tx);
    }
    // the other isolate gets busy again, this one stays busy: it gets its
    // share back
    toChild.send('go');
    bool done = false;
    Future<void> load = () async {
      while (!done) {
        await _round(pool, 16);
      }
    }();
    expect(await msgs.moveNext(), isTrue);
    List<int> used = (msgs.current as List<Object?>).cast<int>();
    done = true;
    await load;
    // the last half of its rounds: about its share (4), not 1 or 2
    List<int> late = used.sublist(used.length ~/ 2);
    expect(
      late.where((int n) => n >= 3).length,
      greaterThanOrEqualTo(late.length * 3 ~/ 4),
      reason: '$used',
    );
    toChild.send('done');
    await msgs.cancel();
    expect(await count(app), lessThanOrEqualTo(8));
    pool.close();
    await settle(app, 0);
  });

  test('a busy isolate gives one to an isolate with all its share in '
      'transactions', () async {
    String app = 'dpg_stuck_$pid';
    PgNativePool pool = openPool(app, 8);
    ReceivePort fromChild = ReceivePort();
    StreamIterator<Object?> msgs = StreamIterator<Object?>(fromChild);
    await Isolate.spawn(_idleThenBusy, <Object>[
      pool.handle,
      fromChild.sendPort,
    ]);
    expect(await msgs.moveNext(), isTrue);
    SendPort toChild = msgs.current as SendPort;
    expect(await msgs.moveNext(), isTrue);
    // the other isolate is busy for 1.5 s, and takes the whole pool
    toChild.send('go');
    await Future<void>.delayed(const Duration(milliseconds: 200));
    // this isolate gets its share (4), all in transactions
    List<int> txs = await Future.wait(<Future<int>>[
      for (int i = 0; i < 4; i++) pool.begin(),
    ]).timeout(const Duration(seconds: 1));
    // one more query, and no other request comes: it must not wait for
    // the transactions or for the request timeout
    PgNativeResult r = await pool
        .query('SELECT 7 AS n')
        .timeout(const Duration(seconds: 1));
    expect(r.rows.single['n'], 7);
    for (int tx in txs) {
      await pool.commit(tx);
    }
    expect(await msgs.moveNext(), isTrue); // its rounds
    toChild.send('done');
    await msgs.cancel();
    pool.close();
    await settle(app, 0);
  });

  test('an isolate with idle connections does not starve another', () async {
    String app = 'dpg_starve_$pid';
    PgNativePool pool = openPool(app, 2);
    ReceivePort fromChild = ReceivePort();
    StreamIterator<Object?> msgs = StreamIterator<Object?>(fromChild);
    await Isolate.spawn(_holder, <Object>[pool.handle, fromChild.sendPort]);
    expect(await msgs.moveNext(), isTrue);
    SendPort toChild = msgs.current as SendPort;
    // the other isolate owns both connections now, and is idle
    expect(await msgs.moveNext(), isTrue);
    expect(msgs.current, 'holding');
    expect(await count(app), 2);
    PgNativeResult r = await pool
        .query('SELECT 7 AS n')
        .timeout(const Duration(seconds: 5));
    expect(r.rows.single['n'], 7);
    // a transaction needs a connection of its own: it gets one too
    int tx = await pool.begin().timeout(const Duration(seconds: 5));
    await pool.query('SELECT 1', transaction: tx);
    await pool.commit(tx);
    expect(await count(app), lessThanOrEqualTo(2));
    toChild.send('done');
    expect(await msgs.moveNext(), isTrue);
    expect(msgs.current, 'bye');
    await msgs.cancel();
    pool.close();
    await settle(app, 0);
  });

  test('a dead isolate frees its connections', () async {
    String app = 'dpg_death_$pid';
    PgNativePool pool = openPool(app, 1);
    ReceivePort fromChild = ReceivePort();
    StreamIterator<Object?> msgs = StreamIterator<Object?>(fromChild);
    Isolate child = await Isolate.spawn(_inTx, <Object>[
      pool.handle,
      fromChild.sendPort,
    ]);
    expect(await msgs.moveNext(), isTrue);
    expect(msgs.current, 'in tx');
    expect(await count(app), 1);
    // it dies in its transaction, without commit or rollback
    child.kill(priority: Isolate.immediate);
    await msgs.cancel();
    // the helper sees it is gone and closes its connection: the one
    // connection of the pool is free again
    int tx = await pool.begin().timeout(const Duration(seconds: 5));
    await pool.query('SELECT 1', transaction: tx);
    await pool.rollback(tx);
    expect(await count(app), 1);
    pool.close();
    await settle(app, 0);
  });

  test('a transaction id works in another isolate', () async {
    PgNativePool pool = openPool('dpg_xtx_$pid', 2);
    await pool.query('DROP TABLE IF EXISTS pgn_xtx');
    await pool.query('CREATE TABLE pgn_xtx (v int)');
    ReceivePort fromChild = ReceivePort();
    StreamIterator<Object?> msgs = StreamIterator<Object?>(fromChild);
    await Isolate.spawn(_beginHere, <Object>[pool.handle, fromChild.sendPort]);
    expect(await msgs.moveNext(), isTrue);
    SendPort toChild = msgs.current as SendPort;
    expect(await msgs.moveNext(), isTrue);
    int tx = msgs.current as int;
    // the other isolate started it and inserted 1; this one goes on
    await pool.query('INSERT INTO pgn_xtx VALUES (2)', transaction: tx);
    PgNativeResult inside = await pool.query(
      'SELECT count(*) AS n FROM pgn_xtx',
      transaction: tx,
    );
    expect(inside.rows.single['n'], 2);
    expect(
      (await pool.query('SELECT count(*) AS n FROM pgn_xtx')).rows.single['n'],
      0,
      reason: 'not committed yet',
    );
    await pool.commit(tx);
    PgNativeResult all = await pool.query('SELECT v FROM pgn_xtx ORDER BY v');
    expect(all.rows, <Map<String, dynamic>>[
      <String, dynamic>{'v': 1},
      <String, dynamic>{'v': 2},
    ]);
    await expectLater(
      pool.query('SELECT 1', transaction: tx),
      throwsA(
        isA<PgException>().having(
          (PgException e) => e.message,
          'message',
          'the transaction is closed',
        ),
      ),
    );
    toChild.send('done');
    expect(await msgs.moveNext(), isTrue);
    await msgs.cancel();
    await pool.query('DROP TABLE pgn_xtx');
    pool.close();
  });

  test('a transaction of a dead isolate is closed', () async {
    PgNativePool pool = openPool('dpg_xtx_dead_$pid', 2);
    ReceivePort fromChild = ReceivePort();
    StreamIterator<Object?> msgs = StreamIterator<Object?>(fromChild);
    Isolate child = await Isolate.spawn(_beginHere, <Object>[
      pool.handle,
      fromChild.sendPort,
      'no table',
    ]);
    expect(await msgs.moveNext(), isTrue);
    expect(await msgs.moveNext(), isTrue);
    int tx = msgs.current as int;
    child.kill(priority: Isolate.immediate);
    await msgs.cancel();
    await expectLater(
      pool
          .query('SELECT 1', transaction: tx)
          .timeout(const Duration(seconds: 5)),
      throwsA(
        isA<PgException>().having(
          (PgException e) => e.message,
          'message',
          'the transaction is closed',
        ),
      ),
    );
    pool.close();
  });

  test('timeouts: a slow query, and an idle transaction', () async {
    String app = 'dpg_timeout_$pid';
    PgNativePool pool = openPool(app, 2);
    dpgSetTimeout(pool.handle, 500);
    await expectLater(
      pool.query('SELECT pg_sleep(3)'),
      throwsA(
        isA<PgException>().having(
          (PgException e) => e.message,
          'message',
          contains('timeout'),
        ),
      ),
    );
    // its connection closed: a new one works
    expect((await pool.query('SELECT 1 AS n')).rows.single['n'], 1);
    int tx = await pool.begin();
    await pool.query('SELECT 1', transaction: tx);
    await Future<void>.delayed(const Duration(milliseconds: 1500));
    // rolled back after the timeout, its connection is a normal one again
    await expectLater(
      pool.query('SELECT 1', transaction: tx),
      throwsA(
        isA<PgException>().having(
          (PgException e) => e.message,
          'message',
          'the transaction is closed',
        ),
      ),
    );
    expect(await count(app), lessThanOrEqualTo(2));
    PgNativeResult open = await watcher.query(
      'SELECT count(*) AS n FROM pg_stat_activity WHERE application_name = @a '
      "AND state LIKE 'idle in transaction%'",
      parameters: <String, String>{'a': app},
    );
    expect(open.rows.single['n'], 0);
    pool.close();
    await settle(app, 0);
  });
}

/// runs in another isolate: 100 short queries at once on the shared pool
Future<int> _sleepyRun(int handle, int w) async {
  PgNativePool pool = PgNativePool.fromHandle(handle);
  List<PgNativeResult> res = await Future.wait(<Future<PgNativeResult>>[
    for (int i = 0; i < 100; i++)
      pool.query(
        r'SELECT pg_sleep(0.002), $1::int AS n',
        parameters: <int>[w * 100 + i],
      ),
  ]);
  int sum = 0;
  for (PgNativeResult r in res) {
    sum += r.rows.single['n'] as int;
  }
  return sum;
}

/// runs in another isolate: after [wait] ms, rounds of 8 queries at once
/// until 1.8 s after the start. Gives each round as [ms since the start,
/// the server pid of each query].
Future<List<List<int>>> _spreadRun(int handle, int wait) async {
  Stopwatch time = Stopwatch()..start();
  await Future<void>.delayed(Duration(milliseconds: wait));
  PgNativePool pool = PgNativePool.fromHandle(handle);
  List<List<int>> rounds = <List<int>>[];
  while (time.elapsedMilliseconds < 1800) {
    List<PgNativeResult> res = await Future.wait(<Future<PgNativeResult>>[
      for (int i = 0; i < 8; i++)
        pool.query('SELECT pg_backend_pid() AS p, pg_sleep(0.005)'),
    ]);
    rounds.add(<int>[
      time.elapsedMilliseconds,
      for (PgNativeResult r in res) r.rows.single['p'] as int,
    ]);
  }
  return rounds;
}

/// a closure with only [handle] and [wait], so it can go to another isolate
Future<List<List<int>>> Function() _spread(int handle, int wait) =>
    () => _spreadRun(handle, wait);

/// a closure with only [handle] and [w], so it can go to another isolate
Future<int> Function() _sleepy(int handle, int w) =>
    () => _sleepyRun(handle, w);

/// Opens both connections of the pool, then stays idle until told.
Future<void> _holder(List<Object> args) async {
  PgNativePool pool = PgNativePool.fromHandle(args[0] as int);
  SendPort out = args[1] as SendPort;
  ReceivePort inbox = ReceivePort();
  out.send(inbox.sendPort);
  await Future.wait(<Future<PgNativeResult>>[
    pool.query('SELECT pg_sleep(0.2)'),
    pool.query('SELECT pg_sleep(0.2)'),
  ]);
  out.send('holding');
  await inbox.first;
  out.send('bye');
}

/// [n] queries at once. Gives how many connections (server pids) they
/// used.
Future<int> _round(PgNativePool pool, int n) async {
  List<PgNativeResult> res = await Future.wait(<Future<PgNativeResult>>[
    for (int i = 0; i < n; i++)
      pool.query('SELECT pg_backend_pid() AS p, pg_sleep(0.01)'),
  ]);
  return res.map((PgNativeResult r) => r.rows.single['p']).toSet().length;
}

/// Opens 4 connections and goes idle. On 'go', runs rounds of 16 queries
/// for 1.5 s and sends the connections each round used. Stays until
/// 'done'.
Future<void> _idleThenBusy(List<Object> args) async {
  PgNativePool pool = PgNativePool.fromHandle(args[0] as int);
  SendPort out = args[1] as SendPort;
  ReceivePort inbox = ReceivePort();
  StreamIterator<Object?> msgs = StreamIterator<Object?>(inbox);
  out.send(inbox.sendPort);
  await Future.wait(<Future<PgNativeResult>>[
    for (int i = 0; i < 4; i++) pool.query('SELECT pg_sleep(0.1)'),
  ]);
  out.send('holding');
  await msgs.moveNext(); // go
  List<int> used = <int>[];
  Stopwatch time = Stopwatch()..start();
  while (time.elapsedMilliseconds < 1500) {
    used.add(await _round(pool, 16));
  }
  out.send(used);
  await msgs.moveNext(); // done
  await msgs.cancel();
}

/// Starts a transaction and stays in it.
Future<void> _inTx(List<Object> args) async {
  PgNativePool pool = PgNativePool.fromHandle(args[0] as int);
  SendPort out = args[1] as SendPort;
  int tx = await pool.begin();
  await pool.query('SELECT 1', transaction: tx);
  out.send('in tx');
  await ReceivePort().first; // never comes
}

/// Starts a transaction (inserts 1 in it, unless a third arg is given),
/// sends its id, and stays alive until told.
Future<void> _beginHere(List<Object> args) async {
  PgNativePool pool = PgNativePool.fromHandle(args[0] as int);
  SendPort out = args[1] as SendPort;
  ReceivePort inbox = ReceivePort();
  out.send(inbox.sendPort);
  int tx = await pool.begin();
  if (args.length < 3) {
    await pool.query('INSERT INTO pgn_xtx VALUES (1)', transaction: tx);
  }
  out.send(tx);
  await inbox.first;
  out.send('bye');
}
