import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:dox_postgres_native/dox_postgres_native.dart';
import 'package:test/test.dart';

// Postgres like in CI. DB_PORT=5472 dart test, for another port.
String host = Platform.environment['DB_HOST'] ?? 'localhost';
int port = int.parse(Platform.environment['DB_PORT'] ?? '5432');

PgNativePool openNative() => PgNativePool.open(
  host: host,
  port: port,
  user: 'postgres',
  password: 'postgres',
  database: 'postgres',
);

void main() {
  late PgNativePool native;

  setUpAll(() async {
    native = openNative();
    await native.query('DROP TABLE IF EXISTS pgn_types');
    await native.query('''
      CREATE TABLE pgn_types (
        id serial PRIMARY KEY,
        i2 int2, i4 int4, i8 int8, f4 float4, f8 float8, num numeric(20, 6),
        t text, vc varchar(20), bp char(3), b bool,
        ts timestamp, tz timestamptz, d date,
        j json, jb jsonb, u uuid, by bytea,
        ai int4[], at text[], ab bool[], af float8[], ats timestamptz[],
        iv interval, tm time
      )''');
  });

  tearDownAll(() async {
    await native.query('DROP TABLE IF EXISTS pgn_types');
  });

  Map<String, dynamic> values = <String, dynamic>{
    'i2': 7,
    'i4': -2147483648,
    'i8': 9007199254740993,
    'f4': 1.5,
    'f8': 3.14159,
    'num': '12345.678900',
    't': 'héllo \' " \\ wörld',
    'vc': 'short',
    'bp': 'ab',
    'b': true,
    'ts': DateTime.utc(2024, 2, 29, 13, 45, 1, 123, 456),
    'tz': DateTime.utc(1999, 12, 31, 23, 59, 59),
    'd': DateTime.utc(2024, 1, 2),
    'j': <String, dynamic>{
      'a': 1,
      'b': <dynamic>[true, null, 'x'],
    },
    'jb': <String, dynamic>{
      'nested': <String, dynamic>{'k': 'v'},
    },
    'u': 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    'by': Uint8List.fromList(<int>[0, 1, 2, 255]),
    'ai': <int?>[1, null, 3],
    'at': <String>['a', 'b c', 'd"e', r'f\g', ''],
    'ab': <bool>[true, false],
    'af': <double>[1.25, -2.5],
    'ats': <DateTime>[DateTime.utc(2020, 5, 6, 7, 8, 9)],
    'iv': const Duration(days: 1, hours: 2, microseconds: 3),
    'tm': '13:45:01.5',
  };
  String cols = values.keys.join(', ');
  String marks = values.keys.map((String k) => '@$k').join(', ');

  test('every type goes in and comes back', () async {
    await native.query(
      'INSERT INTO pgn_types ($cols) VALUES ($marks)',
      parameters: values,
    );
    await native.query('INSERT INTO pgn_types (t) VALUES (NULL)');
    List<Map<String, dynamic>> n = (await native.query(
      'SELECT * FROM pgn_types ORDER BY id',
    )).rows;
    expect(n, hasLength(2));
    Map<String, dynamic> row = Map<String, dynamic>.of(n[0])..remove('id');
    // bpchar keeps its padding
    expect(row, equals(<String, dynamic>{...values, 'bp': 'ab '}));
    expect(
      row['tz'],
      isA<DateTime>().having((DateTime t) => t.isUtc, 'utc', true),
    );
    expect(row['by'], isA<Uint8List>());
    expect(n[1]['t'], isNull);
    expect(n[1]['ai'], isNull);
  });

  test('numeric keeps all its digits', () async {
    List<String> same = <String>[
      '0',
      '12345.678900',
      '10000',
      '100000000.5',
      '-42',
      '0.00001234',
      '-0.5',
      '123456789012345678901234567890.123456789',
      'NaN',
    ];
    for (String v in same) {
      PgNativeResult r = await native.query(
        'SELECT @v::numeric AS n',
        parameters: <String, dynamic>{'v': v},
      );
      expect(r.rows.single['n'], v);
    }
  });

  test('expressions, null params and the select order', () async {
    PgNativeResult r = await native.query(
      'SELECT @a::int + 1 AS x, @b::text AS y, @c AS z, 2 AS a, 1 AS b, '
      "'@not' AS lit, NULL::int AS nothing, @t::bool AS yes",
      parameters: <String, dynamic>{'a': 41, 'b': null, 'c': 'zz', 't': true},
    );
    Map<String, dynamic> row = r.rows.single;
    expect(row.keys.toList(), <String>[
      'x',
      'y',
      'z',
      'a',
      'b',
      'lit',
      'nothing',
      'yes',
    ]);
    expect(row, <String, dynamic>{
      'x': 42,
      'y': null,
      'z': 'zz',
      'a': 2,
      'b': 1,
      'lit': '@not',
      'nothing': null,
      'yes': true,
    });
  });

  test('positional params, and a value it can not send', () async {
    PgNativeResult r = await native.query(
      r'SELECT $1::int AS a, $2 AS b',
      parameters: <Object?>[5, 'x'],
    );
    expect(r.rows.single, <String, dynamic>{'a': 5, 'b': 'x'});
    expect(r.columns, <String>['a', 'b']);
    expect(
      () => native.query(r'SELECT $1', parameters: <Object?>[Object()]),
      throwsArgumentError,
    );
    // the bad request was not sent: the next one still works
    PgNativeResult ok = await native.query('SELECT 1 AS n');
    expect(ok.rows.single['n'], 1);
  });

  test('the cached plan is renewed after ALTER TABLE', () async {
    await native.query('DROP TABLE IF EXISTS pgn_alter');
    await native.query('CREATE TABLE pgn_alter (a int)');
    await native.query('INSERT INTO pgn_alter VALUES (1)');
    String sql = 'SELECT * FROM pgn_alter';
    expect((await native.query(sql)).rows.single, <String, dynamic>{'a': 1});
    await native.query("ALTER TABLE pgn_alter ADD COLUMN b text DEFAULT 'x'");
    expect((await native.query(sql)).rows.single, <String, dynamic>{
      'a': 1,
      'b': 'x',
    });
    await native.query('DROP TABLE pgn_alter');
  });

  test('a big result (sent without a copy) and int params', () async {
    // over 64 KiB: Rust gives the bytes to Dart as external typed data
    PgNativeResult r = await native.query(
      "SELECT g, repeat('x', 100) AS t FROM generate_series(1, 2000) g",
    );
    expect(r.rows, hasLength(2000));
    expect(r.rows.last, <String, dynamic>{'g': 2000, 't': 'x' * 100});
    PgNativeResult n = await native.query(
      r'SELECT $1::int8 AS a, $2::int4 AS b, $3::text AS c',
      parameters: <Object>[-9223372036854775808, -7, 1234567890],
    );
    expect(n.rows.single, <String, dynamic>{
      'a': -9223372036854775808,
      'b': -7,
      'c': '1234567890',
    });
  });

  group('errors', () {
    Future<Object> fail(Future<Object?> Function() run) async {
      try {
        await run();
      } catch (e) {
        return e;
      }
      throw TestFailure('no error');
    }

    test('unique violation has the server fields', () async {
      await native.query('DROP TABLE IF EXISTS pgn_uniq');
      await native.query('CREATE TABLE pgn_uniq (k int PRIMARY KEY)');
      await native.query('INSERT INTO pgn_uniq VALUES (1)');
      Object n = await fail(
        () => native.query(
          'INSERT INTO pgn_uniq VALUES (@k)',
          parameters: <String, dynamic>{'k': 1},
        ),
      );
      expect(n, isA<PgServerException>());
      PgServerException e = n as PgServerException;
      expect(e.code, '23505');
      expect(e.severity, 'ERROR');
      expect(e.constraint, 'pgn_uniq_pkey');
      expect(e.table, 'pgn_uniq');
      expect(e.schema, 'public');
      expect(e.detail, 'Key (k)=(1) already exists.');
      expect(e.message, contains('duplicate key'));
      expect('$e', contains('23505'));
      expect('$e', contains('pgn_uniq_pkey'));
      await native.query('DROP TABLE pgn_uniq');
    });

    test('syntax error and missing table', () async {
      Map<String, String> codes = <String, String>{
        'SELEC 1': '42601',
        'SELECT * FROM pgn_nope': '42P01',
      };
      for (MapEntry<String, String> c in codes.entries) {
        Object n = await fail(() => native.query(c.key));
        expect(n, isA<PgServerException>());
        expect((n as PgServerException).code, c.value);
      }
    });

    test('a missing or an unknown parameter', () async {
      expect(
        () => native.query('SELECT @a', parameters: <String, dynamic>{}),
        throwsArgumentError,
      );
      expect(
        () => native.query('SELECT 1', parameters: <String, dynamic>{'x': 1}),
        throwsArgumentError,
      );
      // same count of names, but a wrong one
      expect(
        () => native.query('SELECT @a', parameters: <String, dynamic>{'b': 1}),
        throwsArgumentError,
      );
    });

    test('a bad server is a PgException', () async {
      PgNativePool bad = PgNativePool.open(
        host: host,
        port: 1,
        user: 'postgres',
        database: 'postgres',
      );
      Object e = await fail(() => bad.query('SELECT 1'));
      expect(e, isA<PgException>());
      bad.close();
      Object closed = await fail(() => bad.query('SELECT 1'));
      expect('$closed', contains('closed'));
    });

    test('a wrong password is a fatal PgServerException', () async {
      PgNativePool bad = PgNativePool.open(
        host: host,
        port: port,
        user: 'postgres',
        password: 'wrong',
        database: 'postgres',
      );
      Object e = await fail(() => bad.query('SELECT 1'));
      expect(e, isA<PgServerException>());
      expect((e as PgServerException).code, '28P01');
      expect(e.severity, 'FATAL');
      bad.close();
    });

    test('sslMode require on a server without TLS is a PgException', () async {
      PgNativePool tls = PgNativePool.open(
        host: host,
        port: port,
        user: 'postgres',
        password: 'postgres',
        database: 'postgres',
        sslMode: 'require',
      );
      Object e = await fail(() => tls.query('SELECT 1'));
      expect(e, isA<PgException>());
      expect(e, isNot(isA<PgServerException>()));
      expect('$e', contains('TLS'));
      tls.close();
      // prefer falls back to no TLS
      PgNativePool prefer = PgNativePool.fromUrl(
        'postgres://postgres:postgres@$host:$port/postgres?sslmode=prefer',
      );
      expect((await prefer.query('SELECT 1 AS n')).rows.single['n'], 1);
      prefer.close();
      expect(
        () => PgNativePool.open(user: 'u', database: 'd', sslMode: 'verify-ca'),
        throwsArgumentError,
      );
    });
  });

  group('transactions', () {
    setUp(() async {
      await native.query('DROP TABLE IF EXISTS pgn_tx');
      await native.query('CREATE TABLE pgn_tx (v int)');
    });

    Future<List<int>> all() async => <int>[
      for (Map<String, dynamic> r in (await native.query(
        'SELECT v FROM pgn_tx ORDER BY v',
      )).rows)
        r['v'] as int,
    ];

    test('commit and rollback, on one connection', () async {
      int tx = await native.begin();
      await native.query('INSERT INTO pgn_tx VALUES (1)', transaction: tx);
      expect(await all(), isEmpty, reason: 'not committed yet');
      // many queries at once still run in order on the connection
      await Future.wait(<Future<Object?>>[
        for (int i = 2; i < 12; i++)
          native.query('INSERT INTO pgn_tx VALUES ($i)', transaction: tx),
      ]);
      PgNativeResult inside = await native.query(
        'SELECT count(*) AS n FROM pgn_tx',
        transaction: tx,
      );
      expect(inside.rows.single['n'], 11);
      await native.commit(tx);
      expect(await all(), hasLength(11));

      int tx2 = await native.begin();
      await native.query('DELETE FROM pgn_tx', transaction: tx2);
      await native.rollback(tx2);
      expect(await all(), hasLength(11));
      expect(
        () => native.query('SELECT 1', transaction: tx2),
        throwsA(isA<PgException>()),
      );
    });

    test('a request sent after the end still gets an answer', () async {
      // Sent in one batch: Rust got the second end and the query while the
      // first end still ran. They waited forever.
      int tx = await native.begin();
      List<Future<Object?>> after = <Future<Object?>>[
        native.rollback(tx),
        native.rollback(tx),
        native.query('SELECT 1', transaction: tx),
      ];
      List<Object?> out = await Future.wait(<Future<Object?>>[
        for (Future<Object?> f in after)
          f.then<Object?>((_) => 'ok', onError: (Object e) => e),
      ]).timeout(const Duration(seconds: 5));
      expect(out.first, 'ok');
      expect(out.skip(1), everyElement(isA<PgException>()));
    });
  });

  test('queries share a connection, a transaction still gets one', () async {
    PgNativePool small = PgNativePool.open(
      host: host,
      port: port,
      user: 'postgres',
      password: 'postgres',
      database: 'postgres',
      maxConnections: 2,
    );
    // many more queries than connections: each gets its own answer
    Future<List<Object?>> load() => Future.wait(<Future<Object?>>[
      for (int i = 0; i < 2000; i++)
        small
            .query(r'SELECT $1::int AS n', parameters: <int>[i])
            .then((PgNativeResult r) => r.rows.single['n']),
    ]);
    Future<List<Object?>> busy = load();
    // the shared connections do not keep a transaction waiting
    int tx = await small.begin().timeout(const Duration(seconds: 5));
    await small.query('SELECT 1', transaction: tx);
    await small.commit(tx);
    expect(await busy, <int>[for (int i = 0; i < 2000; i++) i]);
    small.close();
  });

  test('a burst, then idle, then one query still finishes', () async {
    // named and positional queries mixed in one burst, each with its answer
    List<Object?> burst = await Future.wait(<Future<Object?>>[
      for (int i = 0; i < 500; i++)
        (i.isEven
                ? native.query(
                    'SELECT @n::int AS n',
                    parameters: <String, int>{'n': i},
                  )
                : native.query(r'SELECT $1::int AS n', parameters: <int>[i]))
            .then((PgNativeResult r) => r.rows.single['n']),
    ]);
    expect(burst, <int>[for (int i = 0; i < 500; i++) i]);
    // the Rust side is idle now: a single query must still wake it
    await Future<void>.delayed(const Duration(milliseconds: 200));
    PgNativeResult one = await native
        .query('SELECT 7 AS n')
        .timeout(const Duration(seconds: 5));
    expect(one.rows.single['n'], 7);
  });

  test('a value Dart can not decode fails only its own query', () async {
    // Out of the DateTime range. It threw in the middle of a result batch,
    // so the other results of that batch were lost.
    List<Future<Object?>> all = <Future<Object?>>[
      for (int i = 0; i < 20; i++)
        i == 10
            ? native.query("SELECT '294276-12-31'::timestamp AS t")
            : native.query('SELECT $i AS n'),
    ];
    List<Object?> out = await Future.wait(<Future<Object?>>[
      for (Future<Object?> f in all)
        f.then<Object?>((Object? r) => r, onError: (Object e) => e),
    ]).timeout(const Duration(seconds: 5));
    expect(out[10], isA<PgException>());
    expect(out.whereType<PgNativeResult>(), hasLength(19));
  });

  test('one pool, used by many isolates', () async {
    await native.query('DROP TABLE IF EXISTS pgn_iso');
    await native.query('CREATE TABLE pgn_iso (id int PRIMARY KEY, w int)');
    int handle = native.handle;
    List<int> sums = await Future.wait(<Future<int>>[
      for (int w = 0; w < 8; w++) Isolate.run(_job(handle, w)),
    ]);
    for (int w = 0; w < 8; w++) {
      expect(sums[w], 200 * w * 1000 + 199 * 200 ~/ 2);
    }
    PgNativeResult count = await native.query(
      'SELECT count(*) AS n FROM pgn_iso',
    );
    expect(count.rows.single['n'], 1600);
    await native.query('DROP TABLE pgn_iso');
  });
}

/// runs in another isolate: 200 inserts at once on the shared pool
Future<int> _insertMany(int handle, int w) async {
  PgNativePool pool = PgNativePool.fromHandle(handle);
  List<PgNativeResult> res = await Future.wait(<Future<PgNativeResult>>[
    for (int i = 0; i < 200; i++)
      pool.query(
        'INSERT INTO pgn_iso VALUES (@id, @w) RETURNING id',
        parameters: <String, dynamic>{'id': w * 1000 + i, 'w': w},
      ),
  ]);
  int sum = 0;
  for (PgNativeResult r in res) {
    sum += r.rows.single['id'] as int;
  }
  return sum;
}

/// a closure with only [handle] and [w], so it can go to another isolate
Future<int> Function() _job(int handle, int w) =>
    () => _insertMany(handle, w);
