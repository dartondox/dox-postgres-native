import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:dox_postgres_native/src/bindings.dart';
import 'package:dox_postgres_native/src/exceptions.dart';
import 'package:dox_postgres_native/src/sql.dart';

/// The rows of one query. Values are Dart types: `int`, `double`, `bool`,
/// `String` (also for `numeric`, `uuid` and `time`), `DateTime` in UTC,
/// `Duration` for `interval`, a decoded JSON value, `Uint8List` for
/// `bytea`, and a `List` for an array.
class PgNativeResult {
  PgNativeResult(this.columns, this.rows);

  /// column names, in the order of the select
  final List<String> columns;

  /// one map per row, column name to value
  final List<Map<String, dynamic>> rows;
}

/// A pool of Postgres connections, owned by Rust.
///
/// ```
/// PgNativePool pool = PgNativePool.open(
///   host: 'localhost', user: 'postgres', password: 'postgres',
///   database: 'postgres',
/// );
/// PgNativeResult r = await pool.query(
///   'SELECT * FROM users WHERE id = @id',
///   parameters: <String, dynamic>{'id': 1},
/// );
/// ```
///
/// Connections open when they are needed. Isolates that open the same
/// settings share one pool in Rust, so `maxConnections` is for the whole
/// process. Or give [handle] to another isolate and use
/// [PgNativePool.fromHandle] there.
class PgNativePool {
  PgNativePool.fromHandle(this.handle);

  /// the pool id in Rust. Works in every isolate of this process.
  final int handle;

  /// true after [close]. The Rust pool can stay open for other users of
  /// the same settings, so this object must refuse new work by itself.
  bool _closed = false;

  Future<T> _send<T>(
    int transaction,
    int kind,
    Uint8List sql,
    List<Object?> values,
  ) {
    if (_closed) {
      return Future<T>.error(StateError('PgNativePool is closed'));
    }
    return _Link.instance.send<T>(handle, transaction, kind, sql, values);
  }

  static bool? _available;
  static String? _loadError;

  /// true when the native library is loaded and works.
  static bool get isAvailable => _available ??= _load();

  /// why [isAvailable] is false, or null.
  static String? get loadError {
    isAvailable;
    return _loadError;
  }

  static bool _load() {
    try {
      int version = dpgVersion();
      if (version != abiVersion) {
        _loadError = 'native library ABI $version, expected $abiVersion';
        return false;
      }
      int rc = dpgInit(NativeApi.initializeApiDLData);
      if (rc != 0) {
        _loadError = 'dpg_init failed ($rc)';
        return false;
      }
      return true;
    } catch (error) {
      // the build hook found no library for this platform
      _loadError = '$error';
      return false;
    }
  }

  /// Makes a pool of up to [maxConnections] connections. The same
  /// settings in another isolate give the same pool.
  ///
  /// A [host] that starts with `/` is a Unix socket folder, like libpq.
  /// Eg. `/cloudsql/project:region:instance` on Google Cloud Run.
  ///
  /// [threads] is the size of the Rust runtime of the whole process. Only
  /// the first pool sets it. 0 runs one `current_thread` runtime on its own
  /// thread.
  ///
  /// [sslMode] is like libpq: `disable` (no TLS), `prefer` (TLS when the
  /// server has it), `require` (TLS, no certificate check) or `verify-full`
  /// (TLS, the certificate and the host name are checked with the Mozilla
  /// roots and the roots of the system).
  factory PgNativePool.open({
    String host = 'localhost',
    int port = 5432,
    required String user,
    String password = '',
    required String database,
    int maxConnections = 10,
    String timeZone = 'UTC',
    String? applicationName,
    int threads = 1,
    String sslMode = 'disable',
  }) {
    // tokio-postgres knows disable, prefer and require. verify-full is
    // require with the certificate check on.
    String? mode = const <String, String>{
      'disable': 'disable',
      'prefer': 'prefer',
      'require': 'require',
      'verify-full': 'require',
    }[sslMode];
    if (mode == null) {
      throw ArgumentError.value(
        sslMode,
        'sslMode',
        'Must be disable, prefer, require or verify-full',
      );
    }
    if (!isAvailable) {
      throw UnsupportedError(
        'dox_postgres_native is not available: $_loadError',
      );
    }
    String config = <String>[
      'host=${_quote(host)}',
      'port=$port',
      'user=${_quote(user)}',
      'password=${_quote(password)}',
      'dbname=${_quote(database)}',
      // the session time zone, UTC by default
      'options=${_quote('-c TimeZone=$timeZone')}',
      // connect timeout in seconds
      'connect_timeout=15',
      'sslmode=$mode',
      if (applicationName != null)
        'application_name=${_quote(applicationName)}',
    ].join(' ');
    Uint8List bytes = utf8.encode(config);
    int handle = dpgConnect(
      bytes.address,
      bytes.length,
      maxConnections,
      threads,
      sslMode == 'verify-full' ? 1 : 0,
    );
    if (handle <= 0) {
      throw ArgumentError('dox_postgres_native: bad connection config');
    }
    return PgNativePool.fromHandle(handle);
  }

  /// Makes a pool from a url like
  /// `postgres://user:password@host:5432/database?sslmode=require`.
  /// Reads `sslmode` and `application_name` from the query. A missing port
  /// is 5432.
  factory PgNativePool.fromUrl(
    String url, {
    int maxConnections = 10,
    String timeZone = 'UTC',
    int threads = 1,
  }) {
    Uri uri = Uri.parse(url);
    if (uri.scheme != 'postgres' && uri.scheme != 'postgresql') {
      throw ArgumentError.value(url, 'url', 'Must start with postgres://');
    }
    int colon = uri.userInfo.indexOf(':');
    String user = colon < 0 ? uri.userInfo : uri.userInfo.substring(0, colon);
    Map<String, String> q = uri.queryParameters;
    return PgNativePool.open(
      host: uri.host.isEmpty ? 'localhost' : uri.host,
      port: uri.hasPort ? uri.port : 5432,
      user: Uri.decodeComponent(user),
      password: colon < 0
          ? ''
          : Uri.decodeComponent(uri.userInfo.substring(colon + 1)),
      database: Uri.decodeComponent(
        uri.path.startsWith('/') ? uri.path.substring(1) : uri.path,
      ),
      maxConnections: maxConnections,
      timeZone: timeZone,
      applicationName: q['application_name'],
      threads: threads,
      sslMode: q['sslmode'] ?? 'disable',
    );
  }

  static String _quote(String v) =>
      "'${v.replaceAll(r'\', r'\\').replaceAll("'", r"\'")}'";

  /// Runs [sql] and returns its rows.
  ///
  /// [parameters] is a map for `@name` parameters, or a list for
  /// `$1, $2...`. A value can be null, `bool`, `int`, `double`, `String`,
  /// `DateTime`, `Duration` (an interval), `Uint8List` (bytea), a `Map`
  /// (json) or a `List` (an array, eg. for `int4[]`; its items can be the
  /// same types, a `Map` item is json). Other types throw [ArgumentError].
  /// All but `Uint8List` go as text without a type, so the server picks
  /// the type from the SQL. Add a cast (`@id::int8`) when it can not.
  ///
  /// Throws [PgServerException] for errors from the server (eg. code
  /// `23505` for a unique violation), [PgException] for others.
  Future<PgNativeResult> query(
    String sql, {
    Object? parameters,
    int transaction = 0,
  }) {
    ParsedSql parsed = parameters is List
        ? ParsedSql.direct(sql)
        : ParsedSql.named(sql);
    List<Object?> values = parsed.bind(parameters);
    return _send<PgNativeResult>(transaction, 0, parsed.sql, values);
  }

  /// Starts a transaction on one connection. Use its id as `transaction`
  /// of [query], then [commit] or [rollback] it.
  Future<int> begin() => _send<int>(0, 1, _noSql, const <Object?>[]);

  static final Uint8List _noSql = Uint8List(0);

  /// Commits the transaction and gives its connection back to the pool.
  Future<void> commit(int transaction) => _end(transaction, 2);

  /// Rolls the transaction back and gives its connection back to the pool.
  Future<void> rollback(int transaction) => _end(transaction, 3);

  Future<void> _end(int transaction, int kind) =>
      _send<void>(transaction, kind, _noSql, const <Object?>[]);

  /// Gives back this open of the pool. Isolates that open the same
  /// settings share one pool, and the last [close] closes it for all of
  /// them. Running queries finish. Call it once per [PgNativePool.open];
  /// a pool from [PgNativePool.fromHandle] is closed by the isolate that
  /// opened it.
  void close() {
    if (_closed) return;
    _closed = true;
    dpgClose(handle);
  }
}

/// The link of this isolate to Rust: one sink for the results, and one
/// buffer for the requests that go out in the next microtask.
class _Link {
  _Link._() {
    _port = RawReceivePort(_onBatch, 'dox_postgres_native');
    _port.keepIsolateAlive = false;
    _sink = dpgOpen(_port.sendPort.nativePort);
  }

  static final _Link instance = _Link._();

  late final RawReceivePort _port;
  late final int _sink;

  /// The waiting requests. The id of a request is its slot here. Rust
  /// answers each id once, so a slot is free again after its answer.
  final List<Completer<Object?>?> _pending = <Completer<Object?>?>[];

  /// the sql of each waiting request, to find the columns it had before
  final List<Uint8List> _sqls = <Uint8List>[];
  final List<int> _free = <int>[];
  int _waiting = 0;

  Uint8List _out = Uint8List(1 << 14);
  late ByteData _outData = ByteData.sublistView(_out);
  int _len = 0;
  bool _flushQueued = false;

  Future<T> send<T>(
    int pool,
    int tx,
    int kind,
    Uint8List sql,
    List<Object?> values,
  ) {
    int id = _free.isEmpty ? _pending.length : _free.last;
    Completer<T> done = Completer<T>();
    // encode first: a bad value throws before anything is queued
    _write(id, pool, tx, kind, sql, values);
    if (id == _pending.length) {
      _pending.add(done);
      _sqls.add(sql);
    } else {
      _free.removeLast();
      _pending[id] = done;
      _sqls[id] = sql;
    }
    if (_waiting++ == 0) _port.keepIsolateAlive = true;
    if (!_flushQueued) {
      _flushQueued = true;
      scheduleMicrotask(_flush);
    }
    return done.future;
  }

  void _ensure(int extra) {
    int need = _len + extra;
    if (need <= _out.length) return;
    int size = _out.length * 2;
    while (size < need) {
      size *= 2;
    }
    _out = Uint8List(size)..setRange(0, _len, _out);
    _outData = ByteData.sublistView(_out);
  }

  void _bytes(List<int> b) {
    _ensure(b.length);
    _out.setRange(_len, _len + b.length, b);
    _len += b.length;
  }

  void _write(
    int id,
    int pool,
    int tx,
    int kind,
    Uint8List sql,
    List<Object?> values,
  ) {
    int start = _len;
    _ensure(32);
    ByteData d = _outData;
    d.setUint64(start, id, Endian.little);
    d.setUint64(start + 8, pool, Endian.little);
    d.setUint64(start + 16, tx, Endian.little);
    d.setUint8(start + 24, kind);
    d.setUint8(start + 25, 0);
    d.setUint16(start + 26, values.length, Endian.little);
    d.setUint32(start + 28, sql.length, Endian.little);
    _len += 32;
    _bytes(sql);
    try {
      for (Object? v in values) {
        _ensure(9);
        int at = _len;
        _len += 9;
        int oid = 0;
        if (v == null) {
          _outData.setInt32(at + 5, -1, Endian.little);
        } else {
          if (v is Uint8List) {
            oid = 17; // bytea, binary: the bytes as they are
            _bytes(v);
          } else {
            _text(v, false);
          }
          _outData.setInt32(at + 5, _len - at - 9, Endian.little);
        }
        _outData.setUint32(at, oid, Endian.little);
        _outData.setUint8(at + 4, oid == 17 ? 1 : 0);
      }
    } catch (_) {
      _len = start; // a bad value: nothing of this request is sent
      rethrow;
    }
  }

  /// Writes [v] in the Postgres text format. [inArray]: as an item of an
  /// array literal, so strings get double quotes.
  void _text(Object? v, bool inArray) {
    if (v is String) {
      if (inArray) return _quoted(v);
      _bytes(utf8.encode(v));
    } else if (v is int) {
      _digits(v);
    } else if (v is double) {
      // NaN, Infinity and 1e+21 are all valid float text
      _bytes(v.toString().codeUnits);
    } else if (v is bool) {
      _bytes(v ? _true : _false);
    } else if (v is DateTime) {
      // ponytail: years before 1 AD are not supported
      String text = v.toUtc().toIso8601String();
      _bytes(inArray ? '"$text"'.codeUnits : text.codeUnits);
    } else if (v is Duration) {
      _digits(v.inMicroseconds);
      _bytes(' microseconds'.codeUnits);
    } else if (v is Map) {
      String text = json.encode(v);
      if (inArray) return _quoted(text);
      _bytes(utf8.encode(text));
    } else if (v is List && v is! Uint8List) {
      _ensure(1);
      _out[_len++] = 0x7b; // {
      for (int i = 0; i < v.length; i++) {
        if (i > 0) _bytes(const <int>[0x2c]); // ,
        Object? item = v[i];
        if (item == null) {
          _bytes(_null);
        } else {
          _text(item, true);
        }
      }
      _bytes(const <int>[0x7d]); // }
    } else {
      throw ArgumentError.value(
        v,
        'parameters',
        'dox_postgres_native can not send a ${v.runtimeType}',
      );
    }
  }

  /// [v] in double quotes, with `\` before `"` and `\`: an array item.
  void _quoted(String v) {
    _bytes(
      utf8.encode('"${v.replaceAll(r'\', r'\\').replaceAll('"', r'\"')}"'),
    );
  }

  static final Uint8List _true = Uint8List.fromList('TRUE'.codeUnits);
  static final Uint8List _false = Uint8List.fromList('FALSE'.codeUnits);
  static final Uint8List _null = Uint8List.fromList('NULL'.codeUnits);

  /// Writes [v] as decimal text, without a String in between.
  void _digits(int v) {
    // the minus of the smallest int64 can not be flipped
    if (v == -0x8000000000000000) return _bytes('$v'.codeUnits);
    _ensure(20);
    Uint8List o = _out;
    int at = _len;
    if (v < 0) {
      o[at++] = 0x2d; // -
      v = -v;
    }
    int n = 1;
    for (int t = v; t >= 10; t ~/= 10) {
      n++;
    }
    for (int i = at + n - 1; i >= at; i--) {
      o[i] = 0x30 + v % 10;
      v ~/= 10;
    }
    _len = at + n;
  }

  void _flush() {
    _flushQueued = false;
    if (_len == 0) return;
    dpgSubmit(_sink, _out.address, _len);
    _len = 0;
    if (_out.length > 1 << 20) {
      _out = Uint8List(1 << 14); // do not keep a big buffer forever
      _outData = ByteData.sublistView(_out);
    }
  }

  // ---- results from Rust ----

  void _onBatch(Object? message) {
    Uint8List b = message as Uint8List;
    ByteData d = ByteData.sublistView(b);
    int p = 0;
    while (p + 13 <= b.length) {
      int id = d.getUint64(p, Endian.little);
      int status = d.getUint8(p + 8);
      int len = d.getUint32(p + 9, Endian.little);
      int at = p + 13;
      p = at + len;
      Completer<Object?>? done = id < _pending.length ? _pending[id] : null;
      if (done == null) continue;
      _pending[id] = null;
      Uint8List sql = _sqls[id];
      _sqls[id] = PgNativePool._noSql;
      _free.add(id);
      _waiting--;
      // one bad record fails its own request, not the rest of the batch
      try {
        switch (status) {
          case 0:
            done.complete(_rows(b, d, at, sql));
          case 1:
            done.completeError(PgServerException.fromFields(_fields(b, d, at)));
          case 2:
            done.completeError(
              PgException(utf8.decode(Uint8List.sublistView(b, at, p))),
            );
          default:
            done.complete(d.getUint64(at, Endian.little));
        }
      } catch (error, stack) {
        done.completeError(
          PgException('dox_postgres_native: can not read the result: $error'),
          stack,
        );
      }
    }
    if (_waiting == 0) _port.keepIsolateAlive = false;
  }

  static Map<int, String> _fields(Uint8List b, ByteData d, int at) {
    int count = b[at++];
    Map<int, String> fields = <int, String>{};
    for (int i = 0; i < count; i++) {
      int len = d.getUint32(at + 1, Endian.little);
      fields[b[at]] = utf8.decoder.convert(b, at + 5, at + 5 + len);
      at += 5 + len;
    }
    return fields;
  }

  /// the columns of the last result of each sql (the `Uint8List` of
  /// [ParsedSql], one per sql text). The same query gives the same columns,
  /// so most results skip the decode. The same name strings also keep
  /// their hash code for the row maps. It goes away with its sql.
  static final Expando<_Columns> _columns = Expando<_Columns>();

  static PgNativeResult _rows(Uint8List b, ByteData d, int at, Uint8List sql) {
    int count = d.getUint16(at, Endian.little);
    int start = at;
    at += 2;
    for (int c = 0; c < count; c++) {
      at += 6 + d.getUint16(at + 4, Endian.little);
    }
    _Columns? known = _columns[sql];
    // the bytes are checked: the columns change after eg. ALTER TABLE
    if (known == null || !_same(known.head, b, start, at)) {
      List<String> names = List<String>.filled(count, '');
      List<int> oids = List<int>.filled(count, 0);
      int p = start + 2;
      for (int c = 0; c < count; c++) {
        oids[c] = d.getUint32(p, Endian.little);
        int len = d.getUint16(p + 4, Endian.little);
        names[c] = utf8.decoder.convert(b, p + 6, p + 6 + len);
        p += 6 + len;
      }
      known = _columns[sql] = _Columns(
        Uint8List.fromList(Uint8List.sublistView(b, start, at)),
        List<String>.unmodifiable(names), // shared by results
        oids,
      );
    }
    List<String> names = known.names;
    List<int> oids = known.oids;
    int rowCount = d.getUint32(at, Endian.little);
    at += 4;
    List<Map<String, dynamic>> rows = List<Map<String, dynamic>>.generate(
      rowCount,
      (_) => <String, dynamic>{},
      growable: false,
    );
    for (int r = 0; r < rowCount; r++) {
      Map<String, dynamic> row = rows[r];
      for (int c = 0; c < count; c++) {
        int len = d.getInt32(at, Endian.little);
        at += 4;
        if (len < 0) {
          row[names[c]] = null;
          continue;
        }
        row[names[c]] = _decode(oids[c], b, d, at, len);
        at += len;
      }
    }
    return PgNativeResult(names, rows);
  }

  /// true when [b] from [start] to [end] has the bytes of [head]
  static bool _same(Uint8List head, Uint8List b, int start, int end) {
    if (head.length != end - start) return false;
    for (int i = 0; i < head.length; i++) {
      if (head[i] != b[start + i]) return false;
    }
    return true;
  }

  static const int _y2kMicros = 946684800000000;
  static const int _dayMicros = 86400000000;
  static const int _infinity = 0x7fffffffffffffff;
  static final Converter<List<int>, Object?> _json = utf8.decoder.fuse(
    json.decoder,
  );
  static const String _hex = '0123456789abcdef';

  /// One binary value. A type it does not know comes back as its binary
  /// bytes (`Uint8List`).
  static Object? _decode(int oid, Uint8List b, ByteData d, int at, int len) {
    switch (oid) {
      case 16: // bool
        return b[at] != 0;
      case 20: // int8
        return d.getInt64(at);
      case 23: // int4
        return d.getInt32(at);
      case 21: // int2
        return d.getInt16(at);
      case 25 || 1043 || 19 || 1042: // text, varchar, name, bpchar
        return utf8.decoder.convert(b, at, at + len);
      case 701: // float8
        return d.getFloat64(at);
      case 700: // float4
        return d.getFloat32(at);
      case 1114 || 1184: // timestamp, timestamptz
        int micros = d.getInt64(at);
        if (micros != _infinity && micros != -_infinity - 1) {
          return DateTime.fromMicrosecondsSinceEpoch(
            _y2kMicros + micros,
            isUtc: true,
          );
        }
        return micros > 0 ? 'infinity' : '-infinity';
      case 1082: // date
        int days = d.getInt32(at);
        if (days != 0x7fffffff && days != -0x80000000) {
          return DateTime.fromMicrosecondsSinceEpoch(
            _y2kMicros + days * _dayMicros,
            isUtc: true,
          );
        }
        return days > 0 ? 'infinity' : '-infinity';
      case 2950 when len == 16: // uuid
        return _uuid(b, at);
      case 1700: // numeric
        return _numeric(d, at);
      case 114: // json
        return _json.convert(Uint8List.sublistView(b, at, at + len));
      case 3802 when len > 0 && b[at] == 1: // jsonb, version 1
        return _json.convert(Uint8List.sublistView(b, at + 1, at + len));
      case 1186 when len == 16: // interval
        // ponytail: a month is 30 days, like Postgres justify_days
        return Duration(
          microseconds: d.getInt64(at),
          days: d.getInt32(at + 8) + 30 * d.getInt32(at + 12),
        );
      case 1083: // time, as text like Postgres: 13:45:01.5
        return _time(d.getInt64(at));
      case 26: // oid
        return d.getUint32(at);
      case 18 || 142: // "char", xml
        return utf8.decoder.convert(b, at, at + len);
      case 1000 || // bool[]
          1005 || // int2[]
          1007 || // int4[]
          1016 || // int8[]
          1009 || // text[]
          1015 || // varchar[]
          1014 || // bpchar[]
          1021 || // float4[]
          1022 || // float8[]
          1231 || // numeric[]
          1182 || // date[]
          1115 || // timestamp[]
          1185 || // timestamptz[]
          1183 || // time[]
          1187 || // interval[]
          2951 || // uuid[]
          199 || // json[]
          3807 || // jsonb[]
          1001 || // bytea[]
          1003: // name[]
        return _array(b, d, at);
    }
    // bytea, and the types it does not know. A copy: the value must not
    // keep the whole batch alive.
    return Uint8List.fromList(Uint8List.sublistView(b, at, at + len));
  }

  /// An array: i32 dimensions, i32 has nulls, u32 item type, per dimension
  /// i32 size and i32 lower bound, then per item i32 length and bytes. More
  /// dimensions give lists in lists.
  static List<Object?> _array(Uint8List b, ByteData d, int at) {
    int dims = d.getInt32(at);
    int oid = d.getUint32(at + 8);
    if (dims == 0) return <Object?>[];
    List<int> sizes = <int>[
      for (int i = 0; i < dims; i++) d.getInt32(at + 12 + 8 * i),
    ];
    int p = at + 12 + 8 * dims;
    List<Object?> read(int dim) {
      return List<Object?>.generate(sizes[dim], (_) {
        if (dim + 1 < dims) return read(dim + 1);
        int len = d.getInt32(p);
        p += 4;
        if (len < 0) return null;
        Object? v = _decode(oid, b, d, p, len);
        p += len;
        return v;
      });
    }

    return read(0);
  }

  /// micros since midnight as `HH:MM:SS`, with `.ffffff` when it has them
  static String _time(int micros) {
    String two(int v) => v.toString().padLeft(2, '0');
    int s = micros ~/ 1000000;
    int f = micros % 1000000;
    String out = '${two(s ~/ 3600)}:${two(s ~/ 60 % 60)}:${two(s % 60)}';
    if (f == 0) return out;
    String frac = f.toString().padLeft(6, '0').replaceFirst(RegExp(r'0+$'), '');
    return '$out.$frac';
  }

  /// `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, lower case
  static String _uuid(Uint8List b, int at) {
    List<int> out = List<int>.filled(36, 0x2d); // -
    int o = 0;
    for (int i = 0; i < 16; i++) {
      if (i == 4 || i == 6 || i == 8 || i == 10) o++;
      int v = b[at + i];
      out[o++] = _hex.codeUnitAt(v >> 4);
      out[o++] = _hex.codeUnitAt(v & 15);
    }
    return String.fromCharCodes(out);
  }

  /// numeric as text, with all its digits. Postgres sends groups of 4
  /// decimal digits: the first one times 10000^weight.
  static String _numeric(ByteData d, int at) {
    int count = d.getInt16(at);
    int weight = d.getInt16(at + 2);
    int sign = d.getUint16(at + 4);
    int scale = d.getInt16(at + 6);
    if (sign == 0xc000) return 'NaN';
    if (sign == 0xd000) return 'Infinity';
    if (sign == 0xf000) return '-Infinity';
    int group(int i) => i >= 0 && i < count ? d.getInt16(at + 8 + 2 * i) : 0;
    StringBuffer out = StringBuffer(sign == 0x4000 ? '-' : '');
    if (weight < 0) {
      out.write('0');
    } else {
      out.write(group(0)); // no leading zeros
      for (int i = 1; i <= weight; i++) {
        out.write(group(i).toString().padLeft(4, '0'));
      }
    }
    if (scale > 0) {
      StringBuffer fraction = StringBuffer();
      for (int i = weight + 1; fraction.length < scale; i++) {
        fraction.write(group(i).toString().padLeft(4, '0'));
      }
      out
        ..write('.')
        ..write(fraction.toString().substring(0, scale));
    }
    return out.toString();
  }
}

/// The columns of one result: the bytes they came in, names and type oids.
class _Columns {
  _Columns(this.head, this.names, this.oids);

  final Uint8List head;
  final List<String> names;
  final List<int> oids;
}

/// Decodes one binary value of type [oid] like a result row does. For
/// tests.
Object? decodeValue(int oid, Uint8List bytes) =>
    _Link._decode(oid, bytes, ByteData.sublistView(bytes), 0, bytes.length);
