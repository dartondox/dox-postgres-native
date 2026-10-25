/// An error of the driver: no connection, a timeout, a closed
/// transaction, a value it can not read.
class PgException implements Exception {
  PgException(this.message);

  /// what went wrong
  final String message;

  @override
  String toString() => 'PgException: $message';
}

/// An error that the Postgres server sent, eg. a unique violation.
class PgServerException extends PgException {
  PgServerException(
    super.message, {
    required this.code,
    required this.severity,
    this.detail,
    this.hint,
    this.schema,
    this.table,
    this.column,
    this.constraint,
  });

  /// Makes it from the error fields of the server, by their one letter
  /// code (eg. `C` for the code). See "Error and Notice Message Fields" in
  /// the Postgres docs.
  factory PgServerException.fromFields(Map<int, String> f) {
    return PgServerException(
      f[0x4d] ?? '', // M
      code: f[0x43] ?? '', // C
      // V is not translated, S can be. Old servers only send S.
      severity: f[0x56] ?? f[0x53] ?? '',
      detail: f[0x44], // D
      hint: f[0x48], // H
      schema: f[0x73], // s
      table: f[0x74], // t
      column: f[0x63], // c
      constraint: f[0x6e], // n
    );
  }

  /// the SQLSTATE code, eg. `23505` for a unique violation
  final String code;

  /// `ERROR`, `FATAL` or `PANIC`
  final String severity;

  final String? detail;
  final String? hint;
  final String? schema;
  final String? table;
  final String? column;
  final String? constraint;

  @override
  String toString() {
    StringBuffer out = StringBuffer(
      'PgServerException: $severity $code: $message',
    );
    if (detail != null) out.write(', detail: $detail');
    if (hint != null) out.write(', hint: $hint');
    if (table != null) out.write(', table: $table');
    if (column != null) out.write(', column: $column');
    if (constraint != null) out.write(', constraint: $constraint');
    return out.toString();
  }
}
