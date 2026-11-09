## Unreleased

- Fix: each download try of the native library now has one time limit of 60 s, from connect to the last byte. Before, there was no limit while waiting for the answer, and the 60 s limit for the body only counted the gap between two chunks. So a slow server could hang the build for a long time. Now 3 tries take about 3 minutes at most.

## 3.0.4

- New: the user define `skip: true` turns off the build hook. The library is not downloaded or built. Use it in an app that does not use this database. Without it, nothing changes. If the library is missing, the error now says so, and tells you to remove `skip`.
- Fix: the build hook now tries the download of the native library up to 3 times (waits 1 s, then 3 s). Before, one bad answer from GitHub (eg. 503 or 504) meant no native library, and the app stopped at start. It tries again on a 5xx or 429 status, a connection error or timeout, and a wrong sha256. A 404 is not tried again. If all 3 tries fail, nothing changes: the hook falls back to cargo or adds no library.

## 3.0.3

- Fix: a short or broken server message, or a negative length from Dart, no longer aborts the Dart process. Before, a Rust panic killed the whole process. Now you get an error.
- Fix: an unknown `verify` value in `dpg_connect` now means the full check (`verify-full`). Before, any value other than 1 or 2 turned the certificate check off. Dart only sends 0, 1 or 2, so apps see no change.
- Fix: a text param could be stored with the wrong type after a table was changed. Example: a column was `int`, the table was dropped and made again with a `text` column, then `'007'` was inserted with the same SQL. It was stored as `'7'`, and `WHERE v = '007'` failed with "operator does not exist". The cause: a param without a type gets its type when the statement is prepared, and Postgres keeps that type when it plans the statement again. Now a `CREATE`, `ALTER` or `DROP` on any connection of the pool makes every connection prepare its statements again. Normal queries do no extra work. A `DO` block counts as a schema change too. A schema change from another process, or inside a function, is not seen for the param types. A changed result type is still caught there: Postgres says "cached plan must not change result type", and the query is prepared again and sent once more.
- A query in a transaction is no longer sent again after "cached plan must not change result type" (or "prepared statement does not exist"). The error already ended the transaction, so the second try could only fail with "current transaction is aborted". Now you get the real error.

## 3.0.2

- Fix: when the server ended a connection, its last error and the close could come in one read. The driver then missed the close, and the query on it waited for the 300 s timeout. It now reads on to the end when the server closes.
- CI: the tests also run on a release tag. A red test stops the release.

## 3.0.1

- Faster. All Postgres work now runs on the thread of the Dart isolate, with mio, like `dox_http_native`. There is no tokio thread any more, so no thread hop per query. Queries are written to the socket when Dart sends them, and read when Dart asks for the results. `GET /db` on 2 cores went from 149k to 261k requests per second (Bun: 193k).
- No more tokio, tokio-postgres, deadpool-postgres, tokio-postgres-rustls and futures-util. The driver speaks the Postgres protocol itself, with `postgres-protocol` for the messages and the login (trust, cleartext, md5, SCRAM-SHA-256). Connect and login do not block the isolate. TLS is still rustls, with the same `sslMode` and `sslRootCert` rules.
- `maxConnections` is still the limit for the whole process. When one isolate waits for a connection, the other isolates give their idle ones to the pool. No isolate waits forever while another keeps connections it does not use.
- A dead isolate (eg. killed, or an uncaught error) now closes its connections within about 100 ms. An open transaction of it is rolled back by the server.
- The `threads` option of `PgNativePool.open` and `fromUrl` does nothing now. It stays so code that sets it still works.
- A transaction id still works in any isolate. Its requests go to the isolate that started it, and the answers come back.
- Less memory. A connection reads a chunk, then its messages, then the next chunk, so its read buffer stays small (64 KiB) also when many answers come at once. Read and write buffers that grew over 256 KiB are freed once empty. Row buffers are reused.
- The host name is looked up when the pool opens, and again on a background thread after a connect fails (eg. after a database failover moves the name to another IP). The isolate never waits for DNS.
- The native ABI version is now 6 (`dpg_connect` has no `threads`; `dpg_open`, `dpg_submit` and `dpg_run` work on the link of one isolate; the leaf calls never call the Dart API).

## 3.0.0

- The first stable release.

## 3.0.0-alpha.8

- New `sslRootCert` option of `PgNativePool.open`: the path of a PEM file with the CA certificates to trust, like libpq `sslrootcert`. Use it for a private CA, eg. AWS RDS, Google Cloud SQL, Azure, or a self-signed dev setup. With it, only the certificates in that file are trusted. `fromUrl` reads `sslrootcert=` from the url.
- New `sslMode` `verify-ca`: the certificate chain is checked, the host name is not. `verify-full` checks both. `prefer` and `require` still check nothing.
- A missing, unreadable or empty `sslRootCert` file, or one with no valid certificate, throws a `PgException` at `open` that names the file.
- The root cert path is part of the pool sharing key.
- The native ABI version is now 4 (`dpg_connect` takes the check mode, the root cert path and an error buffer).

## 3.0.0-alpha.7

- The package moved to its own repository: https://github.com/dartondox/dox-postgres-native. The ready-built libraries are on its releases (tag `v<version>`).

- Breaking: isolates that open the same settings (and the same `maxConnections`) now share one pool in Rust, like `dox_mysql_native`. So `maxConnections` is for the whole process, not per isolate. Before, 8 isolates with the default of 10 could open 80 connections. Each `open` counts, and only the last `close()` closes the pool. The native ABI version is now 2.
- The prepared statement cache of a connection drops the statement used longest ago when it is full (LRU, 512 statements). Before, it dropped all statements at once.
- Fewer copies of the result bytes. Rust writes each result straight into the buffer of the next post. A batch of 64 KiB or more goes to Dart without a copy (external typed data).
- Faster decode of `uuid`, `numeric`, `date`, `json` and `jsonb`, without the type registry of the `postgres` package. A negative `numeric` below 1 now has its leading zero: `-0.5`, not `-.5` like the `postgres` package.
- An `int` parameter is written as text straight into the request buffer, without a `String`. It still goes without a type, so the server picks it.
- The column names are kept per SQL text, not only for the last result. Queries that take turns no longer decode their column names again.
- Breaking: no more `postgres` package. This package now has its own `@name` parser, value encoder and errors. Dox has no Dart driver to fall back to.
- Breaking: errors are `PgException` (with `message`) and `PgServerException` (with `code`, `severity`, `detail`, `hint`, `schema`, `table`, `column`, `constraint`). The `ServerException` classes of the `postgres` package are gone.
- Breaking: parameter values are `null`, `bool`, `int`, `double`, `String`, `DateTime`, `Duration`, `Uint8List` (bytea), `Map` (json) and `List` (array). Another type throws `ArgumentError`. `TypedValue` is gone: use a cast in the SQL (`@id::int8` or `@id:int8`).
- Breaking: some result types changed. `interval` is a `Duration` (a month is 30 days), `time` is a `String` like `13:45:01.5`, an infinite date or timestamp is `'infinity'` or `'-infinity'`, and a type it does not know is its binary bytes (`Uint8List`).
- TLS with rustls. New `sslMode` option: `disable` (default), `prefer`, `require` or `verify-full`. `verify-full` checks the certificate and host name with the Mozilla roots and the roots of the system. The TLS settings are part of the pool sharing key.
- New `PgNativePool.fromUrl('postgres://user:pass@host:5432/db?sslmode=require')`.
- An error without a server code now tells its cause, eg. `error performing TLS handshake: invalid peer certificate: UnknownIssuer`.
- The native ABI version is now 3 (`dpg_connect` takes the certificate check flag).

## 3.0.0-alpha.6

- Version bump only, to match the other packages.

## 3.0.0-alpha.5

- The hook comment says how to fill the sha256 list from a release: `dart run bin/native_hashes.dart <tag>`. The list stays empty until the first release.
- A `host` that starts with `/` is a Unix socket folder (eg. Cloud SQL `/cloudsql/project:region:instance`). A test checks it.

## 3.0.0-alpha.4

- Less work per request. Rust copies a batch of requests once and keeps slices of it, instead of one copy of the SQL and of each parameter. A batch of plain queries uses one timeout timer, not one per query. Dart keeps waiting requests in a list, not a map.

## 3.0.0-alpha.3

- Column names and types are decoded once per result shape. The next result with the same columns reuses them.
- Breaking: this package no longer depends on `dox_orm`. `PostgresNativeDriver` moved into `dox_orm`, which now uses it as its default Postgres driver. This package keeps `PgNativePool`.
- Fix: a request never got an answer in two cases. A value that Dart can
  not decode (eg. a timestamp out of the `DateTime` range) lost every other
  result of the same batch. A request sent to a transaction after its end
  (eg. a second rollback) was dropped. Both now fail with a clear error.
- A request that gets no answer in 5 minutes fails with a timeout error
  (like `queryTimeout` of the `postgres` package). Connect timeout is 15 s.
  A transaction that Dart does not end in 5 minutes is rolled back, so its
  connection goes back to the pool.
- `tool/stress.dart`: a stress test that finds requests that never finish.
- Faster queries. Plain queries now share connections: tokio-postgres
  sends the queries of one connection together (pipelining). One tokio task
  runs each batch from Dart, not one task per query. The SQL bytes are
  cached, and binding named parameters builds no extra `Set`.

## 3.0.0-alpha.2

- First release.
- Native Postgres driver: Rust (tokio-postgres, deadpool-postgres) owns the
  connections. Your code stays in Dart.
- `PostgresNativeDriver` for the Dox ORM. Same types and exceptions as the
  `postgres` package.
- `PgNativePool`: queries, transactions, one pool for many isolates.
