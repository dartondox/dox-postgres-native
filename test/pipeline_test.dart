import 'dart:io';

import 'package:dox_postgres_native/dox_postgres_native.dart';
import 'package:test/test.dart';

// Direct mode only: DOX_PG_DIRECT=1 DB_PORT=5452 dart test
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
    skip: Platform.environment['DOX_PG_DIRECT'] == '1'
        ? false
        : 'needs DOX_PG_DIRECT=1',
  );
}
