import 'dart:typed_data';

import 'package:dox_postgres_native/src/pool.dart';
import 'package:test/test.dart';

// No database needed: the binary values are made by hand.
Uint8List bytes(void Function(ByteData d) fill, int length) {
  Uint8List b = Uint8List(length);
  fill(ByteData.sublistView(b));
  return b;
}

void main() {
  test('uuid, date, time and interval', () {
    Uint8List uuid = Uint8List.fromList(<int>[
      for (int i = 0; i < 16; i++) i * 17,
    ]);
    expect(decodeValue(2950, uuid), '00112233-4455-6677-8899-aabbccddeeff');
    // days since 2000-01-01
    expect(
      decodeValue(1082, bytes((ByteData d) => d.setInt32(0, 60), 4)),
      DateTime.utc(2000, 3),
    );
    expect(
      decodeValue(1082, bytes((ByteData d) => d.setInt32(0, 0x7fffffff), 4)),
      'infinity',
    );
    int micros = ((13 * 60 + 45) * 60 + 1) * 1000000 + 500000;
    expect(
      decodeValue(1083, bytes((ByteData d) => d.setInt64(0, micros), 8)),
      '13:45:01.5',
    );
    Uint8List interval = bytes((ByteData d) {
      d.setInt64(0, 7200000000); // 2 hours
      d.setInt32(8, 1); // 1 day
      d.setInt32(12, 1); // 1 month
    }, 16);
    expect(decodeValue(1186, interval), const Duration(days: 31, hours: 2));
  });

  test('an int4[] with a null, and an unknown type as bytes', () {
    Uint8List array = bytes((ByteData d) {
      d.setInt32(0, 1); // dimensions
      d.setInt32(4, 1); // has nulls
      d.setUint32(8, 23); // int4
      d.setInt32(12, 3); // size
      d.setInt32(16, 1); // lower bound
      d.setInt32(20, 4);
      d.setInt32(24, 7);
      d.setInt32(28, -1); // null
      d.setInt32(32, 4);
      d.setInt32(36, -2);
    }, 40);
    expect(decodeValue(1007, array), <int?>[7, null, -2]);
    expect(decodeValue(99999, Uint8List.fromList(<int>[1, 2])), <int>[1, 2]);
  });
}
