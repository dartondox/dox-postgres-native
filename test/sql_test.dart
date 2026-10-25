import 'package:dox_postgres_native/src/sql.dart';
import 'package:test/test.dart';

// No database needed.

/// the new SQL, then ` | ` and the names
String rewrite(String sql) {
  List<String> names = <String>[];
  return '${ParsedSql.rewrite(sql, names)} | ${names.join(' ')}';
}

void main() {
  test(r'@name becomes $n, the same name the same $n', () {
    expect(rewrite('SELECT @a, @b_2, @a'), r'SELECT $1, $2, $1 | a b_2');
    expect(rewrite('SELECT 1'), 'SELECT 1 | ');
  });

  test('no parameters in strings, quoted names and comments', () {
    expect(
      rewrite("SELECT '@a', 'it''s @b', @c"),
      r"SELECT '@a', 'it''s @b', $1 | c",
    );
    expect(rewrite(r"SELECT E'\' @a', @b"), r"SELECT E'\' @a', $1 | b");
    // a plain string has no backslash escapes
    expect(rewrite(r"SELECT '\', @a"), r"SELECT '\', $1 | a");
    expect(
      rewrite(r'SELECT "@a" FROM "x""@y", @b'),
      r'SELECT "@a" FROM "x""@y", $1 | b',
    );
    expect(
      rewrite(r'SELECT $$ @a $$, $tag$ @b $ $tag$, @c'),
      r'SELECT $$ @a $$, $tag$ @b $ $tag$, $1 | c',
    );
    expect(
      rewrite('SELECT 1 -- @a\n, @b /* @c */ , @d'),
      'SELECT 1 -- @a\n, \$1 /* @c */ , \$2 | b d',
    );
  });

  test('casts, types and operators', () {
    expect(rewrite('SELECT @a::int, x::text'), r'SELECT $1::int, x::text | a');
    expect(rewrite('SELECT @a:int8'), r'SELECT $1::int8 | a');
    expect(
      rewrite("SELECT j @> '{}', '{}' <@ j, @ -5, t @@ q, @@x"),
      r"SELECT j @> '{}', '{}' <@ j, @ -5, t @@ q, @$1 | x",
    );
    expect(() => rewrite('SELECT @a: 1'), throwsFormatException);
  });

  test('bind checks the names', () {
    ParsedSql p = ParsedSql.named('SELECT @a, @b, @a');
    expect(p.bind(<String, Object?>{'b': 2, 'a': null}), <Object?>[null, 2]);
    expect(() => p.bind(<String, Object?>{'a': 1}), throwsArgumentError);
    expect(
      () => p.bind(<String, Object?>{'a': 1, 'b': 2, 'c': 3}),
      throwsArgumentError,
    );
    expect(() => p.bind(null), throwsArgumentError);
    expect(() => p.bind(5), throwsArgumentError);
    ParsedSql none = ParsedSql.named('SELECT 1');
    expect(none.bind(null), isEmpty);
    expect(none.bind(<String, Object?>{}), isEmpty);
    expect(() => none.bind(<String, Object?>{'x': 1}), throwsArgumentError);
    // the same SQL is parsed once
    expect(identical(ParsedSql.named('SELECT 1'), none), isTrue);
    expect(
      () => ParsedSql.direct(r'SELECT $1').bind(<String, int>{'a': 1}),
      throwsArgumentError,
    );
  });
}
