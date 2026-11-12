import 'dart:async';
import 'dart:io';
import 'dart:isolate';

import 'package:dox_postgres_native/dox_postgres_native.dart';
import 'package:test/test.dart';

// Postgres like in CI. DB_PORT=5472 dart test, for another port.
String host = Platform.environment['DB_HOST'] ?? 'localhost';
int port = int.parse(Platform.environment['DB_PORT'] ?? '5432');

/// A pool of its own: the application name makes the settings unique, and
/// lets [count] find its connections.
PgNativePool openPool(
  String app, {
  int max = 2,
  Duration requestTimeout = const Duration(seconds: 300),
  Duration? maxConnectionAge,
  Duration? idleTimeout,
}) => PgNativePool.open(
  host: host,
  port: port,
  user: 'postgres',
  password: 'postgres',
  database: 'postgres',
  maxConnections: max,
  applicationName: app,
  requestTimeout: requestTimeout,
  maxConnectionAge: maxConnectionAge,
  idleTimeout: idleTimeout,
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

Future<int> backend(PgNativePool pool) async =>
    (await pool.query('SELECT pg_backend_pid() AS p')).rows.single['p'] as int;

Matcher get cancelled => throwsA(isA<PgQueryCancelledException>());

void main() {
  setUpAll(() => watcher = openPool('dpg_feat_watcher'));
  tearDownAll(() => watcher.close());

  group('pool settings', () {
    test('requestTimeout fails a slow query, the pool still works', () async {
      String app = 'dpg_rt_$pid';
      PgNativePool pool = openPool(
        app,
        requestTimeout: const Duration(milliseconds: 300),
      );
      Stopwatch sw = Stopwatch()..start();
      await expectLater(
        pool.query('SELECT pg_sleep(2)'),
        throwsA(
          isA<PgException>().having(
            (PgException e) => e.message,
            'message',
            contains('timeout, no answer in 300 ms'),
          ),
        ),
      );
      expect(sw.elapsed, lessThan(const Duration(seconds: 3)));
      expect((await pool.query('SELECT 1 AS n')).rows.single['n'], 1);
      pool.close();
      await settle(app, 0);
    });

    test('a timed out query is cancelled on the server', () async {
      String app = 'dpg_rtc_$pid';
      PgNativePool pool = openPool(
        app,
        requestTimeout: const Duration(seconds: 1),
      );
      await expectLater(
        pool.query('SELECT pg_sleep(8)'),
        throwsA(isA<PgException>()),
      );
      // the backend stops long before the 8 s are over
      Stopwatch sw = Stopwatch()..start();
      await settle(app, 0);
      expect(sw.elapsed, lessThan(const Duration(seconds: 4)));
      pool.close();
    });

    test('a bad duration throws', () {
      expect(
        () => openPool('x', requestTimeout: Duration.zero),
        throwsArgumentError,
      );
    });

    test('maxConnectionAge replaces an old connection when free', () async {
      String app = 'dpg_age_$pid';
      PgNativePool pool = openPool(
        app,
        max: 1,
        maxConnectionAge: const Duration(milliseconds: 400),
      );
      int first = await backend(pool);
      // in use past its age: not killed
      await pool.query('SELECT pg_sleep(0.6)');
      // closed once free, without a new query
      await settle(app, 0);
      int second = await backend(pool);
      expect(second, isNot(first));
      // a transaction older than the age keeps its connection
      int tx = await pool.begin();
      int inTx =
          (await pool.query(
                'SELECT pg_backend_pid() AS p',
                transaction: tx,
              )).rows.single['p']
              as int;
      await Future<void>.delayed(const Duration(milliseconds: 600));
      int still =
          (await pool.query(
                'SELECT pg_backend_pid() AS p',
                transaction: tx,
              )).rows.single['p']
              as int;
      expect(still, inTx);
      await pool.commit(tx);
      pool.close();
      await settle(app, 0);
    });

    test('maxConnectionAge works under steady load', () async {
      String app = 'dpg_age_load_$pid';
      PgNativePool pool = openPool(
        app,
        max: 1,
        maxConnectionAge: const Duration(milliseconds: 300),
      );
      Set<int> pids = <int>{};
      List<Future<void>> running = <Future<void>>[];
      Stopwatch sw = Stopwatch()..start();
      // a 20 ms query every 10 ms: the connection is never free
      while (sw.elapsed < const Duration(seconds: 2)) {
        running.add(
          pool.query('SELECT pg_backend_pid() AS p, pg_sleep(0.02)').then((
            PgNativeResult r,
          ) {
            pids.add(r.rows.single['p'] as int);
          }),
        );
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
      await Future.wait(running);
      expect(pids.length, greaterThan(1));
      pool.close();
      await settle(app, 0);
    });

    test('idleTimeout closes a free connection', () async {
      String app = 'dpg_idle_$pid';
      PgNativePool pool = openPool(
        app,
        idleTimeout: const Duration(milliseconds: 300),
      );
      int first = await backend(pool);
      expect(await count(app), 1);
      await settle(app, 0);
      // the pool opens a new one when it needs it
      expect(await backend(pool), isNot(first));
      pool.close();
      await settle(app, 0);
    });
  });

  group('cancel', () {
    late PgNativePool pool;
    String app = 'dpg_cancel_$pid';
    setUp(() => pool = openPool(app, max: 1));
    tearDown(() => pool.close());

    test('a running query stops, the pool still works', () async {
      PgCancelToken token = PgCancelToken();
      Stopwatch sw = Stopwatch()..start();
      Future<PgNativeResult> slow = pool.query(
        'SELECT pg_sleep(10)',
        cancelToken: token,
      );
      await Future<void>.delayed(const Duration(milliseconds: 200));
      token.cancel();
      await expectLater(slow, cancelled);
      expect(sw.elapsed, lessThan(const Duration(seconds: 5)));
      expect(token.isCancelled, isTrue);
      // the one connection is free again
      expect((await pool.query('SELECT 1 AS n')).rows.single['n'], 1);
    });

    test('right after the send, it still stops', () async {
      PgCancelToken token = PgCancelToken();
      Future<PgNativeResult> slow = pool.query(
        'SELECT pg_sleep(10)',
        cancelToken: token,
      );
      // let it go to Rust, but not yet to the server
      await Future<void>.delayed(Duration.zero);
      token.cancel();
      await expectLater(slow.timeout(const Duration(seconds: 5)), cancelled);
    });

    test('before it starts, it does not run', () async {
      await pool.query('DROP TABLE IF EXISTS pgn_cancel');
      await pool.query('CREATE TABLE pgn_cancel (n int)');
      PgCancelToken token = PgCancelToken();
      // cancelled before the flush to Rust
      Future<PgNativeResult> a = pool.query(
        'INSERT INTO pgn_cancel VALUES (1)',
        cancelToken: token,
      );
      token.cancel();
      await expectLater(a, cancelled);
      // a token that is cancelled already
      await expectLater(
        pool.query('INSERT INTO pgn_cancel VALUES (2)', cancelToken: token),
        cancelled,
      );
      // waits for the one connection, cancelled there
      PgCancelToken waiting = PgCancelToken();
      Future<PgNativeResult> busy = pool.query('SELECT pg_sleep(0.5)');
      await Future<void>.delayed(const Duration(milliseconds: 50));
      Future<PgNativeResult> queued = pool.query(
        'INSERT INTO pgn_cancel VALUES (3)',
        cancelToken: waiting,
      );
      await Future<void>.delayed(const Duration(milliseconds: 50));
      waiting.cancel();
      await expectLater(queued, cancelled);
      await busy;
      PgNativeResult r = await pool.query(
        'SELECT count(*) AS n FROM pgn_cancel',
      );
      expect(r.rows.single['n'], 0);
      await pool.query('DROP TABLE pgn_cancel');
    });

    test('after it ended, it does nothing', () async {
      PgCancelToken token = PgCancelToken();
      PgNativeResult r = await pool.query('SELECT 7 AS n', cancelToken: token);
      token.cancel();
      expect(r.rows.single['n'], 7);
      expect((await pool.query('SELECT 1 AS n')).rows.single['n'], 1);
    });

    test('works with queries of another isolate on the pool', () async {
      PgNativePool two = openPool('${app}_iso');
      Future<int> other = Isolate.run(_plainQueries(two.handle));
      List<Future<PgNativeResult>> mine = <Future<PgNativeResult>>[
        for (int i = 0; i < 50; i++) two.query('SELECT pg_sleep(0.001)'),
      ];
      PgCancelToken token = PgCancelToken();
      Future<PgNativeResult> slow = two.query(
        'SELECT pg_sleep(10)',
        cancelToken: token,
      );
      mine.addAll(<Future<PgNativeResult>>[
        for (int i = 0; i < 50; i++) two.query('SELECT pg_sleep(0.001)'),
      ]);
      await Future<void>.delayed(const Duration(milliseconds: 100));
      token.cancel();
      await expectLater(slow, cancelled);
      // no plain query got the cancel
      await Future.wait(mine);
      expect(await other, 200);
      two.close();
    });

    test('a token in a transaction of another isolate is refused', () async {
      PgNativePool two = openPool('${app}_xtx');
      ReceivePort fromChild = ReceivePort();
      StreamIterator<Object?> msgs = StreamIterator<Object?>(fromChild);
      await Isolate.spawn(_beginHere, <Object>[two.handle, fromChild.sendPort]);
      await msgs.moveNext();
      SendPort toChild = msgs.current as SendPort;
      await msgs.moveNext();
      int tx = msgs.current as int;
      Matcher refused = throwsA(
        isA<PgException>().having(
          (PgException e) => e.message,
          'message',
          contains('works only in the isolate that started the transaction'),
        ),
      );
      await expectLater(
        two.query('SELECT 1', transaction: tx, cancelToken: PgCancelToken()),
        refused,
      );
      await expectLater(
        two.stream('SELECT 1', transaction: tx).toList(),
        refused,
      );
      // a plain query still works there
      expect(
        (await two.query('SELECT 1 AS n', transaction: tx)).rows.single['n'],
        1,
      );
      toChild.send('end');
      await msgs.moveNext();
      await msgs.cancel();
      two.close();
    });

    test('in a transaction, the cancel never hits the next query', () async {
      for (int i = 0; i < 150; i++) {
        int tx = await pool.begin();
        PgCancelToken token = PgCancelToken();
        Future<Object?> run(Future<PgNativeResult> f) =>
            f.then<Object?>((PgNativeResult r) => r, onError: (Object e) => e);
        Future<Object?> one = run(pool.query('SELECT 1', transaction: tx));
        Future<Object?> two = run(
          pool.query('SELECT 2', transaction: tx, cancelToken: token),
        );
        Future<Object?> sleep = run(
          pool.query('SELECT pg_sleep(0.1)', transaction: tx),
        );
        await Future<void>.delayed(Duration(microseconds: (i % 15) * 100));
        token.cancel();
        expect(await one, isA<PgNativeResult>());
        Object? b = await two;
        Object? c = await sleep;
        // the sleep query is never the one that gets the cancel
        expect(
          c,
          isNot(
            isA<PgServerException>().having(
              (PgServerException e) => e.code,
              'code',
              '57014',
            ),
          ),
        );
        if (b is PgNativeResult) expect(c, isA<PgNativeResult>());
        await pool.rollback(tx);
      }
    }, timeout: const Timeout(Duration(minutes: 2)));

    test('in a transaction, the transaction is aborted', () async {
      int tx = await pool.begin();
      PgCancelToken token = PgCancelToken();
      Future<PgNativeResult> slow = pool.query(
        'SELECT pg_sleep(10)',
        transaction: tx,
        cancelToken: token,
      );
      await Future<void>.delayed(const Duration(milliseconds: 200));
      token.cancel();
      await expectLater(slow, cancelled);
      await expectLater(
        pool.query('SELECT 1', transaction: tx),
        throwsA(
          isA<PgServerException>().having(
            (PgServerException e) => e.code,
            'code',
            '25P02',
          ),
        ),
      );
      await pool.rollback(tx);
      expect((await pool.query('SELECT 1 AS n')).rows.single['n'], 1);
    });
  });

  group('stream', () {
    late PgNativePool pool;
    String app = 'dpg_stream_$pid';
    setUp(() => pool = openPool(app, max: 1));
    tearDown(() => pool.close());

    test('100k rows in chunks', () async {
      int chunks = 0;
      int rows = 0;
      int sum = 0;
      await for (PgNativeResult r in pool.stream(
        'SELECT g AS n, md5(g::text) AS h FROM generate_series(1, @n) g',
        parameters: <String, int>{'n': 100000},
      )) {
        chunks++;
        expect(r.columns, <String>['n', 'h']);
        expect(r.rows.length, lessThanOrEqualTo(1000));
        rows += r.rows.length;
        for (Map<String, dynamic> row in r.rows) {
          sum += row['n'] as int;
        }
      }
      // the default chunk size is 1000
      expect(chunks, 100);
      expect(rows, 100000);
      expect(sum, 100000 * 100001 ~/ 2);
      // a last chunk that is not full
      List<int> sizes = <int>[
        await for (PgNativeResult r in pool.stream(
          'SELECT g FROM generate_series(1, 25) g',
          chunkSize: 10,
        ))
          r.rows.length,
      ];
      expect(sizes, <int>[10, 10, 5]);
      expect(await pool.stream('SELECT 1 WHERE false').toList(), isEmpty);
    });

    test('a paused listener stops the reads (bounded memory)', () async {
      int got = 0;
      Completer<void> done = Completer<void>();
      late StreamSubscription<PgNativeResult> sub;
      sub = pool
          .stream('SELECT g FROM generate_series(1, 100000) g', chunkSize: 100)
          .listen((PgNativeResult r) {
            got++;
            if (got == 1) sub.pause();
          }, onDone: done.complete);
      await Future<void>.delayed(const Duration(milliseconds: 500));
      // one chunk to the listener, at most one more on the way
      expect(got, lessThanOrEqualTo(2));
      sub.resume();
      await done.future;
      expect(got, 1000);
    });

    test('cancel mid-stream frees the connection', () async {
      int got = 0;
      await for (PgNativeResult _ in pool.stream(
        'SELECT g FROM generate_series(1, 10000000) g',
        chunkSize: 100,
      )) {
        if (++got == 3) break;
      }
      expect(got, 3);
      // the pool has one connection: it is free again
      expect(
        (await pool.query('SELECT 1 AS n').timeout(const Duration(seconds: 5)))
            .rows
            .single['n'],
        1,
      );
    });

    test('a cancel token stops a running stream', () async {
      PgCancelToken token = PgCancelToken();
      List<Object> events = <Object>[];
      Future<void> run = pool
          .stream('SELECT pg_sleep(10), 1 AS n', cancelToken: token)
          .forEach(events.add);
      await Future<void>.delayed(const Duration(milliseconds: 200));
      token.cancel();
      await expectLater(run.timeout(const Duration(seconds: 5)), cancelled);
      expect(events, isEmpty);
      expect((await pool.query('SELECT 1 AS n')).rows.single['n'], 1);
    });

    test('stop early in a transaction, the next query works', () async {
      for (int i = 0; i < 100; i++) {
        int tx = await pool.begin();
        PgNativeResult first = await pool
            .stream(
              'SELECT g FROM generate_series(1, 100000) g',
              transaction: tx,
              chunkSize: 10,
            )
            .first;
        expect(first.rows.length, 10);
        PgNativeResult r = await pool.query('SELECT 1 AS n', transaction: tx);
        expect(r.rows.single['n'], 1);
        await pool.rollback(tx);
      }
    });

    test('a paused stream fails after requestTimeout', () async {
      PgNativePool short = openPool(
        '${app}_pause',
        max: 1,
        requestTimeout: const Duration(milliseconds: 300),
      );
      int got = 0;
      List<Object> errors = <Object>[];
      Completer<void> done = Completer<void>();
      late StreamSubscription<PgNativeResult> sub;
      sub = short
          .stream('SELECT g FROM generate_series(1, 10000) g', chunkSize: 10)
          .listen(
            (PgNativeResult r) {
              got++;
              if (got == 1) sub.pause();
            },
            onError: errors.add,
            onDone: done.complete,
          );
      await Future<void>.delayed(const Duration(seconds: 1));
      sub.resume();
      await done.future.timeout(const Duration(seconds: 5));
      expect(got, lessThanOrEqualTo(2));
      expect(errors.single.toString(), contains('timeout'));
      // its connection closed: a new one works
      expect((await short.query('SELECT 1 AS n')).rows.single['n'], 1);
      short.close();
    });

    test('a stale plan after an outside ALTER is sent again', () async {
      PgNativePool other = openPool('${app}_alter');
      await other.query('DROP TABLE IF EXISTS pgn_stale');
      await other.query('CREATE TABLE pgn_stale (n int)');
      await other.query('INSERT INTO pgn_stale VALUES (1), (2), (3)');
      String sql = 'SELECT * FROM pgn_stale ORDER BY n';
      List<PgNativeResult> before = await pool.stream(sql).toList();
      expect(before.single.columns, <String>['n']);
      // another pool: this one does not see the schema change
      await other.query('ALTER TABLE pgn_stale ADD COLUMN m int');
      List<PgNativeResult> after = await pool
          .stream(sql, chunkSize: 2)
          .toList();
      expect(after.first.columns, <String>['n', 'm']);
      expect(after.expand((PgNativeResult r) => r.rows).length, 3);
      await other.query('DROP TABLE pgn_stale');
      other.close();
    });

    test('errors come as a stream error', () async {
      await expectLater(
        pool
            .stream(
              'SELECT 1 / (g - 5) FROM generate_series(1, 10) g',
              chunkSize: 2,
            )
            .toList(),
        throwsA(
          isA<PgServerException>().having(
            (PgServerException e) => e.code,
            'code',
            '22012',
          ),
        ),
      );
      expect((await pool.query('SELECT 1 AS n')).rows.single['n'], 1);
    });

    test('inside a transaction', () async {
      PgNativePool two = openPool('${app}_tx');
      int tx = await two.begin();
      await two.query('CREATE TEMP TABLE pgn_s (n int)', transaction: tx);
      await two.query(
        'INSERT INTO pgn_s SELECT g FROM generate_series(1, 50) g',
        transaction: tx,
      );
      int seen = 0;
      // a query of the same transaction waits for the stream
      Future<PgNativeResult>? mid;
      await for (PgNativeResult r in two.stream(
        'SELECT n FROM pgn_s ORDER BY n',
        transaction: tx,
        chunkSize: 20,
      )) {
        mid ??= two.query('SELECT count(*) AS c FROM pgn_s', transaction: tx);
        seen += r.rows.length;
      }
      expect(seen, 50);
      expect((await mid!).rows.single['c'], 50);
      await two.rollback(tx);
      two.close();
    });
  });
}

/// runs in another isolate: 200 short queries on the shared pool
Future<int> _plainRun(int handle) async {
  PgNativePool pool = PgNativePool.fromHandle(handle);
  List<PgNativeResult> res = await Future.wait(<Future<PgNativeResult>>[
    for (int i = 0; i < 200; i++)
      pool.query(r'SELECT pg_sleep(0.001), $1::int AS n', parameters: <int>[i]),
  ]);
  return res.length;
}

/// a closure with only [handle], so it can go to another isolate
Future<int> Function() _plainQueries(int handle) =>
    () => _plainRun(handle);

/// Starts a transaction, sends its id, and stays alive until told.
Future<void> _beginHere(List<Object> args) async {
  PgNativePool pool = PgNativePool.fromHandle(args[0] as int);
  SendPort out = args[1] as SendPort;
  ReceivePort inbox = ReceivePort();
  out.send(inbox.sendPort);
  int tx = await pool.begin();
  out.send(tx);
  await inbox.first;
  await pool.rollback(tx);
  out.send('bye');
}
