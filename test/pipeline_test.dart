import 'dart:io';

import 'package:dox_postgres_native/dox_postgres_native.dart';
import 'package:test/test.dart';

// Postgres like in CI. DB_PORT=5472 dart test, for another port.
String host = Platform.environment['DB_HOST'] ?? 'localhost';
int port = int.parse(Platform.environment['DB_PORT'] ?? '5432');

void main() {
  test(
    'many queries at once, errors mixed in, each answer to its own query',
    () async {
      PgNativePool pool = PgNativePool.open(
        host: host,
        port: port,
        user: 'postgres',
        password: 'postgres',
        database: 'postgres',
        maxConnections: 3,
      );
      // the same failing sql many times: a failed Parse must fail only
      // its own query, and the queries sent after it on that statement
      // must get the real error
      Future<Object?> one(int i) {
        Future<PgNativeResult> q;
        if (i % 7 == 3) {
          q = pool.query('SELEC 1');
        } else if (i % 11 == 5) {
          q = pool.query(
            r'SELECT 1 / ($1::int - $1::int)',
            parameters: <int>[i],
          );
        } else if (i.isEven) {
          q = pool.query(r'SELECT $1::int AS n', parameters: <int>[i]);
        } else {
          q = pool.query(
            'SELECT @n::int8 AS n, @t::text AS t',
            parameters: <String, Object>{'n': i, 't': 'x$i'},
          );
        }
        return q.then<Object?>(
          (PgNativeResult r) => r.rows.single,
          onError: (Object e) => e,
        );
      }

      List<Object?> out = await Future.wait(<Future<Object?>>[
        for (int i = 0; i < 2000; i++) one(i),
      ]).timeout(const Duration(seconds: 20));
      for (int i = 0; i < 2000; i++) {
        Object? r = out[i];
        if (i % 7 == 3) {
          expect((r as PgServerException).code, '42601', reason: '$i');
        } else if (i % 11 == 5) {
          expect((r as PgServerException).code, '22012', reason: '$i');
        } else if (i.isEven) {
          expect(r, <String, dynamic>{'n': i}, reason: '$i');
        } else {
          expect(r, <String, dynamic>{'n': i, 't': 'x$i'}, reason: '$i');
        }
      }
      pool.close();
    },
  );

  test(
    'a connection the server ends fails its query, the next works',
    () async {
      PgNativePool pool = PgNativePool.open(
        host: host,
        port: port,
        user: 'postgres',
        password: 'postgres',
        database: 'postgres',
        maxConnections: 1,
        applicationName: 'dpg_killed',
      );
      PgNativePool admin = PgNativePool.open(
        host: host,
        port: port,
        user: 'postgres',
        password: 'postgres',
        database: 'postgres',
        maxConnections: 1,
      );
      Future<Object?> slow = pool
          .query('SELECT pg_sleep(5)')
          .then<Object?>((_) => 'ok', onError: (Object e) => e);
      await Future<void>.delayed(const Duration(milliseconds: 300));
      await admin.query(
        'SELECT pg_terminate_backend(pid) FROM pg_stat_activity '
        "WHERE application_name = 'dpg_killed'",
      );
      Object? e = await slow.timeout(const Duration(seconds: 3));
      expect(e, isA<PgServerException>());
      expect((e as PgServerException).code, '57P01');
      // a new connection opens for the next query
      expect((await pool.query('SELECT 1 AS n')).rows.single['n'], 1);
      pool.close();
      admin.close();
    },
  );

  test('results bigger than the Dart buffer, many times over', () async {
    // Results did not fit Dart's buffer; the call that came after with a
    // bigger buffer found even more, and they were lost (a hang).
    PgNativePool pool = PgNativePool.open(
      host: host,
      port: port,
      user: 'postgres',
      password: 'postgres',
      database: 'postgres',
      maxConnections: 4,
    );
    await pool.query('SELECT 1');
    for (int round = 0; round < 5; round++) {
      List<PgNativeResult> all = await Future.wait(<Future<PgNativeResult>>[
        for (int i = 0; i < 200; i++)
          pool.query(
            r"SELECT g, repeat('x', 100) AS t FROM generate_series(1, 50) g "
            r'WHERE $1::int >= 0',
            parameters: <int>[i],
          ),
      ]).timeout(const Duration(seconds: 10));
      expect(all.every((PgNativeResult r) => r.rows.length == 50), isTrue);
    }
    pool.close();
  });

  group('no prepared statements left on the server after errors', () {
    PgNativePool open({String? applicationName}) => PgNativePool.open(
      host: host,
      port: port,
      user: 'postgres',
      password: 'postgres',
      database: 'postgres',
      // one connection: all queries see the same server statements
      maxConnections: 1,
      applicationName: applicationName,
    );

    late PgNativePool pool;

    Future<int> prepared({int transaction = 0}) async {
      PgNativeResult r = await pool.query(
        'SELECT count(*)::int AS n FROM pg_prepared_statements',
        transaction: transaction,
      );
      return r.rows.first['n'] as int;
    }

    setUp(() async {
      pool = open();
      await pool.query('DROP TABLE IF EXISTS pgn_stmt_leak');
      await pool.query('CREATE TABLE pgn_stmt_leak (id int PRIMARY KEY)');
      await pool.query('INSERT INTO pgn_stmt_leak VALUES (1)');
    });

    tearDown(() async {
      await pool.query('DROP TABLE IF EXISTS pgn_stmt_leak');
      pool.close();
    });

    test('a failed Bind or Execute does not leave one each time', () async {
      int before = await prepared();
      for (int i = 0; i < 30; i++) {
        // all at once: the replies must stay in order
        List<Object?> got = await Future.wait(<Future<Object?>>[
          pool
              .query('INSERT INTO pgn_stmt_leak VALUES (1)')
              .then<Object?>((_) => 'ok', onError: (Object e) => e),
          pool
              .query(r'SELECT $1::int AS n', parameters: <String>['abc'])
              .then<Object?>((_) => 'ok', onError: (Object e) => e),
          pool.query('SELECT $i AS n').then<Object?>((PgNativeResult r) => r),
          pool
              .query('SELEC $i')
              .then<Object?>((_) => 'ok', onError: (Object e) => e),
        ]);
        expect('${got[0]}', contains('23505'));
        expect('${got[1]}', contains('22P02'));
        expect((got[2] as PgNativeResult).rows.first['n'], i);
        expect('${got[3]}', contains('42601'));
      }
      // the failing SELECT stays cached (the INSERT is from setUp), the
      // 30 selects are cached too; nothing else is left
      expect(await prepared(), before + 1 + 30);
      expect((await pool.query('SELECT 7 AS n')).rows.first['n'], 7);
    });

    test('in a failed transaction too', () async {
      int before = await prepared();
      for (int i = 0; i < 10; i++) {
        int tx = await pool.begin();
        await expectLater(
          // not the sql of setUp: a new statement
          pool.query(
            'INSERT INTO pgn_stmt_leak (id) VALUES (1)',
            transaction: tx,
          ),
          throwsA(predicate((Object e) => '$e'.contains('23505'))),
        );
        // the Parse fails (25P02): the statement does not exist
        await expectLater(
          pool.query('SELECT $i', transaction: tx),
          throwsA(predicate((Object e) => '$e'.contains('25P02'))),
        );
        await pool.rollback(tx);
      }
      // the INSERT stays cached: one at most
      expect(await prepared(), lessThanOrEqualTo(before + 1));
      expect((await pool.query('SELECT 7 AS n')).rows.first['n'], 7);
    });

    test('an out of date statement is closed, also in a transaction', () async {
      const String sql = 'SELECT * FROM pgn_stmt_leak';
      await pool.query(sql);
      // another pool changes the table: this pool does not see it
      PgNativePool other = open(applicationName: 'pgn_other');
      await other.query('ALTER TABLE pgn_stmt_leak ADD COLUMN y int');
      other.close();
      int tx = await pool.begin();
      // 0A000, no retry in a transaction: the statement is dropped
      await expectLater(
        pool.query(sql, transaction: tx),
        throwsA(predicate((Object e) => '$e'.contains('0A000'))),
      );
      // the Close goes with this query, in the failed transaction
      await expectLater(
        pool.query('SELECT 1', transaction: tx),
        throwsA(predicate((Object e) => '$e'.contains('25P02'))),
      );
      await pool.rollback(tx);
      PgNativeResult r = await pool.query(
        'SELECT count(*)::int AS n FROM pg_prepared_statements '
        'WHERE statement = @s',
        parameters: <String, Object>{'s': sql},
      );
      expect(r.rows.first['n'], 0);
      expect((await pool.query(sql)).rows.first['y'], isNull);
    });
  });
}
