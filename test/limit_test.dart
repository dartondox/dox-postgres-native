import 'dart:async';
import 'dart:io';
import 'dart:isolate';

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

/// Starts a transaction and stays in it.
Future<void> _inTx(List<Object> args) async {
  PgNativePool pool = PgNativePool.fromHandle(args[0] as int);
  SendPort out = args[1] as SendPort;
  int tx = await pool.begin();
  await pool.query('SELECT 1', transaction: tx);
  out.send('in tx');
  await ReceivePort().first; // never comes
}
