import 'dart:convert';
import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:hooks/hooks.dart';
import 'package:test/test.dart';

/// Runs hook/build.dart with the given user defines. Returns the code assets
/// in its output. A `lib_path` is also given, so the hook has a library to
/// add when `skip` does not stop it. No download and no cargo run.
List<Object?> _assets(Directory tmp, Map<String, Object?> defines) {
  File lib = File('${tmp.path}/lib.bin')..writeAsStringSync('x');
  Uri root = Directory.current.uri;
  BuildInputBuilder builder = BuildInputBuilder()
    ..setupShared(
      packageRoot: root,
      packageName: 'dox_postgres_native',
      outputDirectoryShared: tmp.uri.resolve('shared/'),
      outputFile: tmp.uri.resolve('output.json'),
      userDefines: PackageUserDefines(
        workspacePubspec: PackageUserDefinesSource(
          defines: <String, Object?>{'lib_path': lib.path, ...defines},
          basePath: root,
        ),
      ),
    )
    ..config.setupBuild(linkingEnabled: false)
    ..addExtension(
      CodeAssetExtension(
        targetArchitecture: Architecture.current,
        targetOS: OS.current,
        linkModePreference: LinkModePreference.dynamic,
      ),
    );
  Directory('${tmp.path}/shared').createSync();
  File input = File('${tmp.path}/input.json')
    ..writeAsStringSync(jsonEncode(builder.json));
  ProcessResult result = Process.runSync(Platform.resolvedExecutable, <String>[
    'run',
    'hook/build.dart',
    '--config=${input.path}',
  ]);
  expect(result.exitCode, 0, reason: '${result.stderr}');
  Map<String, Object?> out = jsonDecode(
    File('${tmp.path}/output.json').readAsStringSync(),
  ) as Map<String, Object?>;
  return (out['assets'] as List<Object?>?) ?? <Object?>[];
}

void main() {
  late Directory tmp;
  setUp(() => tmp = Directory.systemTemp.createTempSync('hook_skip'));
  tearDown(() => tmp.deleteSync(recursive: true));

  test('without skip the hook adds the library', () {
    expect(_assets(tmp, <String, Object?>{}), hasLength(1));
  });

  test('skip: true adds no library', () {
    expect(_assets(tmp, <String, Object?>{'skip': true}), isEmpty);
  });

  test('only a real bool true skips', () {
    expect(_assets(tmp, <String, Object?>{'skip': 'true'}), hasLength(1));
    expect(_assets(tmp, <String, Object?>{'skip': false}), hasLength(1));
  });
}
