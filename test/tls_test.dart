import 'dart:io';

import 'package:dox_postgres_native/dox_postgres_native.dart';
import 'package:test/test.dart';

PgNativePool openTls(String host, int port, String mode, String? rootCert) =>
    PgNativePool.open(
      host: host,
      port: port,
      user: 'postgres',
      password: 'postgres',
      database: 'postgres',
      sslMode: mode,
      sslRootCert: rootCert,
    );

/// Runs a command, throws when it fails.
Future<String> run(String cmd, List<String> args, {String? dir}) async {
  ProcessResult r = await Process.run(cmd, args, workingDirectory: dir);
  if (r.exitCode != 0) {
    throw StateError('$cmd ${args.join(' ')}: ${r.stderr}');
  }
  return '${r.stdout}';
}

/// true when [cmd] runs
Future<bool> has(String cmd, List<String> args) async {
  try {
    return (await Process.run(cmd, args)).exitCode == 0;
  } on ProcessException {
    return false;
  }
}

void main() {
  test('a bad sslRootCert fails at open and names the file', () {
    String missing = '${Directory.systemTemp.path}/dpg_no_such_ca.pem';
    expect(
      () => openTls('127.0.0.1', 1, 'verify-full', missing),
      throwsA(
        isA<PgException>().having(
          (PgException e) => e.message,
          'message',
          startsWith('can not read sslRootCert $missing: '),
        ),
      ),
    );
    // require does not check, so it does not read the file
    openTls('127.0.0.1', 1, 'require', missing).close();
  });

  test('fromUrl reads sslrootcert', () {
    expect(
      () => PgNativePool.fromUrl(
        'postgres://u@127.0.0.1:1/d?sslmode=verify-ca&sslrootcert=/no/ca.pem',
      ),
      throwsA(isA<PgException>()),
    );
  });

  // A real server with TLS: a CA and a server cert for `localhost` made
  // with openssl, and postgres:16 in docker on a free port.
  group('postgres with TLS', () {
    late Directory dir;
    late String ca;
    late int port;
    String name = 'dpg_tls_test_$pid';
    bool ready = false;

    setUpAll(() async {
      if (!await has('openssl', <String>['version']) ||
          !await has('docker', <String>['info'])) {
        return;
      }
      dir = await Directory.systemTemp.createTemp('dpg_tls');
      String d = dir.path;
      await run('openssl', <String>[
        'req', '-x509', '-newkey', 'rsa:2048', '-nodes', //
        '-keyout', 'ca.key', '-out', 'ca.crt', '-days', '2',
        '-subj', '/CN=dpg test CA',
        '-addext', 'basicConstraints=critical,CA:TRUE',
        '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
      ], dir: d);
      await run('openssl', <String>[
        'req', '-newkey', 'rsa:2048', '-nodes', //
        '-keyout', 'server.key', '-out', 'server.csr', '-subj', '/CN=localhost',
      ], dir: d);
      File('$d/ext.cnf').writeAsStringSync(
        'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n',
      );
      await run('openssl', <String>[
        'x509', '-req', '-in', 'server.csr', '-CA', 'ca.crt', //
        '-CAkey', 'ca.key', '-CAcreateserial', '-out', 'server.crt',
        '-days', '2', '-extfile', 'ext.cnf',
      ], dir: d);
      ca = '$d/ca.crt';
      // a free port: bind to 0, read it, let it go
      ServerSocket free = await ServerSocket.bind('127.0.0.1', 0);
      port = free.port;
      await free.close();
      // the key must belong to the postgres user, with mode 600
      await run('docker', <String>[
        'run', '-d', '--rm', '--name', name, //
        '-p', '127.0.0.1:$port:5432', '-v', '$d:/in:ro',
        '-e', 'POSTGRES_PASSWORD=postgres',
        '--entrypoint', 'bash', 'postgres:16', '-c',
        'mkdir -p /certs && cp /in/server.crt /in/server.key /certs && '
            'chown postgres /certs/* && chmod 600 /certs/server.key && '
            'exec docker-entrypoint.sh postgres -c ssl=on '
            '-c ssl_cert_file=/certs/server.crt '
            '-c ssl_key_file=/certs/server.key',
      ]);
      // init starts a server without TCP first, so this waits for the real one
      for (int i = 0; i < 120; i++) {
        if (await has('docker', <String>[
          'exec', name, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres', //
        ])) {
          ready = true;
          break;
        }
        await Future<void>.delayed(const Duration(milliseconds: 500));
      }
      if (!ready) throw StateError('postgres with TLS did not start');
    });

    tearDownAll(() async {
      if (!ready) return;
      await Process.run('docker', <String>['rm', '-f', name]);
      await dir.delete(recursive: true);
    });

    Future<Object?> ssl(String host, String mode, String? rootCert) async {
      PgNativePool pool = openTls(host, port, mode, rootCert);
      try {
        PgNativeResult r = await pool.query(
          'SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()',
        );
        return r.rows.first['ssl'];
      } finally {
        pool.close();
      }
    }

    test('verify-full trusts the CA file', () async {
      if (!ready) return markTestSkipped('needs openssl and docker');
      expect(await ssl('localhost', 'verify-full', ca), true);
    });

    test('verify-full without the CA file fails', () async {
      if (!ready) return markTestSkipped('needs openssl and docker');
      await expectLater(
        ssl('localhost', 'verify-full', null),
        throwsA(
          isA<PgException>().having(
            (PgException e) => e.message,
            'message',
            contains('UnknownIssuer'),
          ),
        ),
      );
    });

    test('verify-ca checks the chain, not the host name', () async {
      if (!ready) return markTestSkipped('needs openssl and docker');
      // the cert is for localhost, not 127.0.0.1
      expect(await ssl('127.0.0.1', 'verify-ca', ca), true);
      await expectLater(
        ssl('127.0.0.1', 'verify-full', ca),
        throwsA(isA<PgException>()),
      );
      await expectLater(
        ssl('127.0.0.1', 'verify-ca', null),
        throwsA(isA<PgException>()),
      );
    });
  });
}
