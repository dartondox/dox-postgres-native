# dox_postgres_native

Native Postgres client for Dart. It is the default Postgres driver of the Dox ORM.

[![pub package](https://img.shields.io/pub/v/dox_postgres_native.svg)](https://pub.dev/packages/dox_postgres_native)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/dartondox/dox-postgres-native/blob/main/LICENSE)

Part of [Dox](https://dartondox.dev), a web backend framework for Dart.
Source code: [github.com/dartondox/dox-postgres-native](https://github.com/dartondox/dox-postgres-native).

## Install

You usually do not add this package. `dox_orm` depends on it and uses it by default.
Add it only to use the pool without the ORM:

```bash
dart pub add dox_postgres_native
```

To see what is ready and what may still change, read [Status and roadmap](https://dartondox.dev/latest/concepts/status-and-roadmap/).

## How it works

- Rust owns the connections and the pool.
- The Postgres work of each isolate runs on the thread of that isolate. There is no extra runtime thread, and no thread hop per query.
- Queries are pipelined: many queries go out on one connection without waiting for each answer.
- Isolates that open the same settings share one pool. `maxConnections` is the limit for the whole process. An idle connection moves to an isolate that waits for one.
- Your models and queries stay in Dart. You do not change any config.
- It does not use the Dart `postgres` package. It has its own `@name` parser and value encoder.
- Server errors are `PgServerException`, other errors are `PgException`.

## The native library

A build hook (`hook/build.dart`) gets the Rust library. It tries these in order:

1. A path you set in your app `pubspec.yaml` (`hooks: user_defines: dox_postgres_native: lib_path: ...`).
2. A ready-built library from the GitHub release, checked with sha256.
3. A build from source with `cargo`. This needs [Rust](https://rustup.rs).

The hook has no sha256 list for ready-built libraries yet, because no `dox_postgres_native-v*` release exists yet.
The release workflow builds them for macOS, Linux and Windows on such a tag.
So step 2 is skipped, and you need Rust to get the native driver.
There is no Dart driver to fall back to.

Use `dart run`, `dart test` or `dart build cli`. `dart compile exe` does not run build hooks.

## Use it

```dart
import 'package:dox_postgres_native/dox_postgres_native.dart';

void main() async {
  PgNativePool pool = PgNativePool.open(
    host: 'localhost',
    user: 'postgres',
    password: 'postgres',
    database: 'postgres',
  );
  PgNativeResult result = await pool.query(
    'SELECT id, name FROM users WHERE id = @id',
    parameters: <String, dynamic>{'id': 1},
  );
  print(result.rows); // [{id: 1, name: Ada}]
}
```

Or from a url:

```dart
PgNativePool pool = PgNativePool.fromUrl(
  'postgres://user:password@db.example.com:5432/app?sslmode=verify-full',
);
```

## Pool settings

All are optional. The defaults keep connections open and wait up to 300 s.

```dart
PgNativePool pool = PgNativePool.open(
  host: 'localhost',
  user: 'postgres',
  password: 'postgres',
  database: 'postgres',
  requestTimeout: const Duration(seconds: 30),
  maxConnectionAge: const Duration(minutes: 30),
  idleTimeout: const Duration(minutes: 5),
);
```

- `requestTimeout` (default 300 s): the longest wait for one request (a free connection, the connect and the query). Over it, the request fails with a `PgException` that says `timeout`. A CancelRequest stops the query on the server, and its connection is closed. A transaction with no request for this long is rolled back.
- `maxConnectionAge` (default: no limit): a connection older than this is closed when it is next free. It is never closed while a query or a transaction uses it. Once it is too old it takes no new query, so it is free soon also under steady load. A new one opens when it is needed. Useful behind a load balancer, or to follow a DNS change after a failover.
- `idleTimeout` (default: kept): a free connection that is not used for this long is closed. A new one opens when it is needed.

`fromUrl` takes the same three. Isolates share one pool only when all settings are the same.

## Cancel a query

```dart
PgCancelToken token = PgCancelToken();
Future<PgNativeResult> slow = pool.query('SELECT pg_sleep(60)', cancelToken: token);
token.cancel();
try {
  await slow;
} on PgQueryCancelledException {
  print('stopped');
}
```

- Cancel before the query starts: it does not run.
- Cancel while it runs: the driver sends a Postgres CancelRequest for it, and the future fails with `PgQueryCancelledException`.
- Cancel after it ended: nothing happens. You keep the result.
- A query with a token gets a connection for itself, so the cancel can not stop another query. In a transaction, it is not pipelined: it waits for the queries before it, and the ones after it wait for it.
- In a transaction, a cancelled query is an error, so Postgres aborts the transaction. Roll it back.
- One token can stop many queries. Use it in the isolate that made it.
- A transaction started in another isolate does not take a query with a token. It throws a `PgException`.
- The CancelRequest goes in plain text on a new connection, like libpq before Postgres 17.

## Stream rows

For a big result, `stream` gives the rows in chunks. Each chunk is a `PgNativeResult` with up to `chunkSize` rows (default 1000).

```dart
await for (PgNativeResult chunk in pool.stream(
  'SELECT * FROM events WHERE day = @day',
  parameters: <String, dynamic>{'day': '2026-01-01'},
  chunkSize: 500,
)) {
  for (Map<String, dynamic> row in chunk.rows) {
    // ...
  }
}
```

- The query runs on a server-side portal. The driver asks the server for the next chunk only when your listener is ready for it. So only about two chunks are in memory, also for millions of rows.
- The query starts when you listen. Stop listening (eg. `break` in `await for`, or `.first`) to stop it. Its connection is free again. In a transaction, the stream then ends after the chunk that runs now, with no CancelRequest, so the next query of the transaction is safe.
- `cancelToken` stops it too. Then the stream fails with `PgQueryCancelledException`.
- An error (eg. a `PgServerException`) comes as a stream error, then the stream ends. A plan that is out of date after an `ALTER TABLE` is prepared again and sent once more, like `query`, when no chunk came yet.
- It holds one connection until it ends. A paused listener that waits longer than `requestTimeout` fails the stream with a timeout.
- It works in a transaction (`transaction: tx`). Other queries of that transaction wait until the stream ends. The stream works only in the isolate that started the transaction.

## Parameter values

`null`, `bool`, `int`, `double`, `String`, `DateTime`, `Duration` (an interval), `Uint8List` (bytea), `Map` (json) and `List` (an array, eg. for `int4[]`).
Another type throws `ArgumentError`. So does a map that misses an `@name` of the SQL, or has a name the SQL does not use.

All values but `Uint8List` go as text without a type, so the server picks the type.
Add a cast when it can not, eg. `@id::int8` or `@id:int8`.
For a json column, a `List` is an array, not json: send `jsonEncode(list)`.

## Errors

```dart
try {
  await pool.query('INSERT INTO users (email) VALUES (@e)', parameters: {'e': e});
} on PgServerException catch (error) {
  if (error.code == '23505') print('taken: ${error.constraint}');
}
```

- `PgServerException`: the server said no. It has `code` (SQLSTATE), `severity`, `message`, and `detail`, `hint`, `schema`, `table`, `column`, `constraint` when the server sends them.
- `PgException`: everything else, eg. no connection, a TLS error, a timeout.
- `PgQueryCancelledException` (a `PgException`): the query was stopped with a `PgCancelToken`.

## TLS

`sslMode` of `PgNativePool.open` (or `sslmode` in the url):

- `disable` (the default): no TLS.
- `prefer`: TLS when the server has it, no certificate check.
- `require`: TLS, no certificate check.
- `verify-ca`: TLS, and the certificate chain is checked. The host name is not checked.
- `verify-full`: TLS, and the certificate and the host name are checked.

`verify-ca` and `verify-full` use the Mozilla roots and the roots of the system.
Your database may use a private CA, eg. AWS RDS, Google Cloud SQL, Azure, or a self-signed dev setup.
Then give the CA file with `sslRootCert` (or `sslrootcert` in the url):

```dart
PgNativePool pool = PgNativePool.open(
  host: 'mydb.abc123.eu-west-1.rds.amazonaws.com',
  user: 'app',
  password: 'secret',
  database: 'app',
  sslMode: 'verify-full',
  sslRootCert: '/etc/ssl/rds-global-bundle.pem',
);
```

- It is the path of a PEM file with one or more CA certificates.
- With it, only the certificates in that file are trusted, like libpq.
- A file that is missing, or has no valid certificate, throws a `PgException` that names the file.
- `prefer` and `require` do not use it.

## Skip the download

By default the build hook gets the native library for this package. If your app does not use this database, you can turn that off.
In your app `pubspec.yaml`:

```yaml
hooks:
  user_defines:
    dox_postgres_native:
      skip: true
```

Then no library is downloaded or built. Only a real `true` counts. The text `'true'` in quotes does not skip.
If the app still opens a pool, it throws an error that says the library is not in the build.

## Features

- Many queries go to Rust in one call. Many results come back in one call.
- Each connection keeps its prepared statements.
- Transactions: `pool.begin()`, `pool.commit(tx)`, `pool.rollback(tx)`.
- Row streaming in chunks, with backpressure: `pool.stream(sql)`.
- Query cancel: `PgCancelToken`.
- Pool settings: `requestTimeout`, `maxConnectionAge`, `idleTimeout`.
- Parameters as a map for `@name`, or a list for `$1, $2`.
- TLS with rustls.

## Fuzzing

The code that reads bytes from the server has fuzz targets in `native/fuzz/`. CI runs each one for 60 seconds when `native/` changes.

- `messages`: random bytes as a stream of backend messages. It cuts the frames with `frame` and runs `columns`, `row`, `error_body` and `error_code` on each body. All at once and in small pieces must give the same result. Where the `postgres-protocol` crate reads a DataRow, a RowDescription or an ErrorResponse without an error, our parser must give the same values.
- `rows`: a RowDescription and DataRows built from the input, made into a rows record with `rows_record`. The record must have the exact shape that Dart reads (`_rows` in `lib/src/pool.dart`): its lengths match what follows, and it ends where the length says.
- `conn_read`: the real read loop of a connection (`Conn::messages`), with some requests waiting (queries, a stream, BEGIN, COMMIT), fed with server bytes. No panic. All at once and in pieces give the same records, the same error and the same bytes to send. Every record `ready` has the shape Dart reads. The records are not much bigger than the input. A DataRow with another count of values than the columns, or rows before their columns, is refused.
- `login`: the login of a connection (`Conn::messages` in the `Auth` state): cleartext, MD5 and SCRAM requests, key data, errors, with queries waiting. No panic. All at once and in pieces give the same records, the same error and the same state. The waiting queries are sent when the login ends, not before.

To run one locally:

```sh
rustup toolchain install nightly
cargo install cargo-fuzz --locked
cd native
cp Cargo.lock fuzz/Cargo.lock
mkdir -p /tmp/corpus/messages /tmp/corpus/rows /tmp/corpus/conn_read /tmp/corpus/login
cargo +nightly fuzz run messages /tmp/corpus/messages fuzz/corpus/messages -- -max_total_time=300 -max_len=65536
cargo +nightly fuzz run rows /tmp/corpus/rows fuzz/corpus/rows -- -max_total_time=300 -max_len=65536
cargo +nightly fuzz run conn_read /tmp/corpus/conn_read fuzz/corpus/conn_read -- -max_total_time=300 -max_len=65536
cargo +nightly fuzz run login /tmp/corpus/login fuzz/corpus/login -- -max_total_time=300 -max_len=65536
```

- The first folder gets the new inputs the fuzzer finds. Use a temp folder, so `fuzz/corpus/` keeps only the small seed files. Commit a new seed there only when it is useful.
- A crash stops the run. The input that caused it is in `fuzz/artifacts/<target>/`. Run it again with `cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<file>`.
- The seeds of all targets also run as a unit test (`cargo test`). The `conn_read` input is: one byte for the piece size, one byte for the request count, one byte per request, then the server bytes. The `login` input is: one byte for the piece size, one byte for the count of waiting queries, then the server bytes.
- The fuzz crate builds the same source as `native/`. Keep the dependencies in `native/fuzz/Cargo.toml` the same as in `native/Cargo.toml`. CI checks this. `fallible-iterator` is there only for the fuzz code, which reads `postgres-protocol` messages.

## Documentation

- [Native Postgres driver](https://dartondox.dev/latest/database/postgres-native/)
- [API reference](https://pub.dev/documentation/dox_postgres_native/latest/)

## License

MIT. See [LICENSE](https://github.com/dartondox/dox-postgres-native/blob/main/LICENSE).
