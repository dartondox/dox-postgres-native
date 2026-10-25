import 'dart:convert';
import 'dart:typed_data';

/// SQL with `@name` (or `$1`) parameters, parsed once and cached.
class ParsedSql {
  ParsedSql._(String sql, this._names) : sql = utf8.encode(sql);

  /// the SQL with `$1, $2...` as the server wants it, in UTF-8
  final Uint8List sql;

  /// the `@name` parameters in the order of `$1, $2...`. Null for SQL
  /// that already has `$1, $2...`.
  final List<String>? _names;

  static final Map<String, ParsedSql> _named = <String, ParsedSql>{};
  static final Map<String, ParsedSql> _direct = <String, ParsedSql>{};

  /// Parses [sql] with `@name` parameters.
  static ParsedSql named(String sql) {
    ParsedSql? hit = _named[sql];
    if (hit != null) return hit;
    // ponytail: a simple clear, an LRU if apps send many different queries
    if (_named.length > 1024) _named.clear();
    List<String> names = <String>[];
    String out = rewrite(sql, names);
    return _named[sql] = ParsedSql._(out, names);
  }

  /// [sql] as it is, with `$1, $2...` parameters.
  static ParsedSql direct(String sql) {
    ParsedSql? hit = _direct[sql];
    if (hit != null) return hit;
    if (_direct.length > 1024) _direct.clear();
    return _direct[sql] = ParsedSql._(sql, null);
  }

  /// The values in the order of `$1, $2...`. A map must have exactly the
  /// `@name` parameters of the SQL, or it throws [ArgumentError].
  List<Object?> bind(Object? parameters) {
    List<String>? names = _names;
    if (parameters == null) {
      if (names != null && names.isNotEmpty) {
        throw ArgumentError.value(
          parameters,
          'parameters',
          'Missing variable for `${names.first}`',
        );
      }
      return const <Object?>[];
    }
    if (parameters is List) return parameters;
    if (parameters is! Map) {
      throw ArgumentError.value(
        parameters,
        'parameters',
        'Must either be a list or a map',
      );
    }
    if (names == null) {
      throw ArgumentError.value(
        parameters,
        'parameters',
        r'A map needs @name parameters, not $1',
      );
    }
    List<Object?> out = List<Object?>.filled(names.length, null);
    for (int i = 0; i < names.length; i++) {
      String name = names[i];
      Object? value = parameters[name];
      if (value == null && !parameters.containsKey(name)) {
        throw ArgumentError.value(
          parameters,
          'parameters',
          'Missing variable for `$name`',
        );
      }
      out[i] = value;
    }
    // all names are there, so a longer map has names the SQL lacks
    if (parameters.length != names.length) {
      Iterable<Object?> extra = parameters.keys.where(
        (Object? k) => !names.contains(k),
      );
      throw ArgumentError.value(
        parameters,
        'parameters',
        'Contains superfluous variables: ${extra.join(', ')}',
      );
    }
    return out;
  }

  /// Replaces each `@name` of [sql] with `$1, $2...` and adds the names to
  /// [names], in the order of their first use. The same name twice is the
  /// same `$n`.
  ///
  /// The same rules as `Sql.named` of the `postgres` package: no
  /// parameters in strings ('', E'', $tag$...$tag$), quoted names ("")
  /// and comments (--, /* */). `::` is a cast. An `@` without a name after
  /// it stays (eg. the `@>` and `@@` operators). `@name:type` is a
  /// parameter with a type: it becomes `$1::type`.
  static String rewrite(String sql, List<String> names) {
    StringBuffer out = StringBuffer();
    int n = sql.length;
    int i = 0;
    int copied = 0; // sql[copied..i] is not written yet
    while (i < n) {
      int c = sql.codeUnitAt(i);
      if (c == _quote || c == _doubleQuote) {
        // E'' (C-style escapes) when the quote follows a lone e or E
        bool escapes =
            c == _quote &&
            i > 0 &&
            (sql.codeUnitAt(i - 1) | 0x20) == 0x65 &&
            (i < 2 || !_isWord(sql.codeUnitAt(i - 2)));
        i++;
        while (i < n) {
          int d = sql.codeUnitAt(i++);
          if (escapes && d == _backslash) {
            i++;
          } else if (d == c) {
            break; // '' and "" are a close and an open: still inside
          }
        }
      } else if (c == _minus && _at(sql, i + 1) == _minus) {
        while (i < n &&
            sql.codeUnitAt(i) != 0x0a &&
            sql.codeUnitAt(i) != 0x0d) {
          i++;
        }
      } else if (c == _slash && _at(sql, i + 1) == _star) {
        int end = sql.indexOf('*/', i + 2);
        i = end < 0 ? n : end + 2;
      } else if (c == _dollar) {
        // $tag$...$tag$ or $$...$$. A tag does not start with a digit, so
        // `$1` is a parameter.
        int j = i + 1;
        if (j < n && !_isDigit(sql.codeUnitAt(j))) {
          while (j < n && _isWord(sql.codeUnitAt(j))) {
            j++;
          }
        }
        if (j < n && sql.codeUnitAt(j) == _dollar) {
          String tag = sql.substring(i, j + 1);
          int end = sql.indexOf(tag, j + 1);
          i = end < 0 ? n : end + tag.length;
        } else {
          i++;
        }
      } else if (c == _colon && _at(sql, i + 1) == _colon) {
        i += 2;
      } else if (c == _atSign) {
        int start = i + 1;
        int j = start;
        while (j < n && _isWord(sql.codeUnitAt(j))) {
          j++;
        }
        if (j == start) {
          i++; // no name: not a parameter
          continue;
        }
        String name = sql.substring(start, j);
        int index = names.indexOf(name);
        if (index < 0) {
          names.add(name);
          index = names.length;
        } else {
          index++;
        }
        out
          ..write(sql.substring(copied, i))
          ..write(r'$')
          ..write(index);
        // `@name:type`, but not `@name::type`
        if (_at(sql, j) == _colon && _at(sql, j + 1) != _colon) {
          int t = j + 1;
          while (t < n && _isWord(sql.codeUnitAt(t))) {
            t++;
          }
          if (t == j + 1) {
            throw FormatException('Expected type name after colon', sql, j);
          }
          out
            ..write('::')
            ..write(sql.substring(j + 1, t));
          j = t;
        }
        i = copied = j;
      } else {
        i++;
      }
    }
    if (copied == 0) return sql;
    out.write(sql.substring(copied));
    return out.toString();
  }

  static int _at(String s, int i) => i < s.length ? s.codeUnitAt(i) : -1;

  static bool _isDigit(int c) => c >= 0x30 && c <= 0x39;

  /// a letter, a digit or `_`
  static bool _isWord(int c) =>
      _isDigit(c) || (c | 0x20) >= 0x61 && (c | 0x20) <= 0x7a || c == 0x5f;

  static const int _quote = 0x27; // '
  static const int _doubleQuote = 0x22; // "
  static const int _backslash = 0x5c;
  static const int _minus = 0x2d;
  static const int _slash = 0x2f;
  static const int _star = 0x2a;
  static const int _dollar = 0x24;
  static const int _colon = 0x3a;
  static const int _atSign = 0x40;
}
