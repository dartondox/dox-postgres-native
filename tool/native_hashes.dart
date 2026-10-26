// Fills the `_sha256` map in hook/build.dart from a GitHub release.
// The release workflow (.github/workflows/ci.yaml) runs it before it
// publishes. You can also run it by hand:
//
//   dart run tool/native_hashes.dart v<version>
//
// It downloads `SHA256SUMS` of the release and writes the hashes into
// hook/build.dart.
import 'dart:convert';
import 'dart:io';

Future<void> main(List<String> args) async {
  if (args.length != 1 || !args.first.startsWith('v')) {
    stderr.writeln('usage: dart run tool/native_hashes.dart v<version>');
    exit(64);
  }
  String tag = args.first;
  File hook = File('hook/build.dart');
  if (!hook.existsSync()) {
    stderr.writeln('${hook.path} not found. Run it from the repo root.');
    exit(66);
  }
  String source = hook.readAsStringSync();
  String releases = RegExp(r"const String _releases =\s*'([^']+)';")
      .firstMatch(source)![1]!;
  String package = RegExp(
    r'^name:\s*(\S+)',
    multiLine: true,
  ).firstMatch(File('pubspec.yaml').readAsStringSync())![1]!;

  HttpClient client = HttpClient();
  String sums;
  try {
    Uri url = Uri.parse('$releases/$tag/SHA256SUMS');
    HttpClientResponse res = await (await client.getUrl(url)).close();
    sums = await res.transform(utf8.decoder).join();
    if (res.statusCode != 200) {
      stderr.writeln('download $url: ${res.statusCode}');
      exit(69);
    }
  } finally {
    client.close(force: true);
  }

  // each line: `<sha256>  <asset name>`
  List<String> entries = <String>[];
  for (String line in LineSplitter.split(sums)) {
    List<String> parts = line.trim().split(RegExp(r'\s+\*?'));
    if (parts.length == 2 && parts[1].startsWith('$package-')) {
      entries.add("  '${parts[1]}':\n      '${parts[0]}',\n");
    }
  }
  if (entries.isEmpty) {
    stderr.writeln('no $package files in SHA256SUMS');
    exit(65);
  }
  entries.sort();

  RegExp map = RegExp(
    r'const Map<String, String> _sha256 = <String, String>\{[^}]*\};',
  );
  if (!map.hasMatch(source)) {
    stderr.writeln('no `_sha256` map in ${hook.path}');
    exit(65);
  }
  hook.writeAsStringSync(
    source.replaceFirst(
      map,
      'const Map<String, String> _sha256 = <String, String>{\n'
      '${entries.join()}};',
    ),
  );
  stdout.writeln('${hook.path}: ${entries.length} hashes from $tag');
}
