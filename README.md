# dox_postgres_native

Native Postgres client for Dart. It is the default Postgres driver of the Dox ORM.

[![pub package](https://img.shields.io/pub/v/dox_postgres_native.svg)](https://pub.dev/packages/dox_postgres_native)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/dartondox/dox-postgres-native/blob/main/LICENSE)

Part of [Dox](https://dartondox.dev), a web backend framework for Dart, like Laravel.
Source code: [github.com/dartondox/dox-postgres-native](https://github.com/dartondox/dox-postgres-native).

## Install

You usually do not add this package. `dox_orm` depends on it and uses it by default.
Add it only to use the pool without the ORM:

```bash
dart pub add dox_postgres_native
```

To see what is ready and what may still change, read [Status and roadmap](https://dartondox.dev/latest/concepts/status-and-roadmap/).

## How it works

- Rust ([tokio-postgres](https://docs.rs/tokio-postgres)) owns the connections and the pool.
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

## Features

- Many queries go to Rust in one call. Many results come back in one message.
- Each connection keeps its prepared statements.
- Transactions: `pool.begin()`, `pool.commit(tx)`, `pool.rollback(tx)`.
- Parameters as a map for `@name`, or a list for `$1, $2`.
- TLS with rustls.

## Documentation

- [Native Postgres driver](https://dartondox.dev/latest/database/postgres-native/)
- [API reference](https://pub.dev/documentation/dox_postgres_native/latest/)

## License

MIT. See [LICENSE](https://github.com/dartondox/dox-postgres-native/blob/main/LICENSE).
