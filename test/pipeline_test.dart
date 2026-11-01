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
}
