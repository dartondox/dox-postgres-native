import 'dart:ffi';

/// FFI calls into the Rust library. The build hook (hook/build.dart) ships
/// the library as the code asset of this file.

/// The ABI version the Dart side expects. Must match `ABI_VERSION` in Rust.
const int abiVersion = 6;

@Native<Int32 Function()>(symbol: 'dpg_version', isLeaf: true)
external int dpgVersion();

@Native<Int32 Function(Pointer<Void>)>(symbol: 'dpg_init')
external int dpgInit(Pointer<Void> dlData);

@Native<
  Int64 Function(
    Pointer<Uint8>,
    Int64,
    Int64,
    Int64,
    Pointer<Uint8>,
    Int64,
    Pointer<Uint8>,
    Int64,
  )
>(symbol: 'dpg_connect', isLeaf: true)
external int dpgConnect(
  Pointer<Uint8> cfg,
  int length,
  int maxSize,
  int verify,
  Pointer<Uint8> rootCert,
  int rootLength,
  Pointer<Uint8> error,
  int errorCapacity,
);

@Native<Void Function(Int64)>(symbol: 'dpg_close')
external void dpgClose(int pool);

/// Opens the link of this isolate. Its idle helper posts to [sendPort].
@Native<Int64 Function(Int64)>(symbol: 'dpg_open', isLeaf: true)
external int dpgOpen(int sendPort);

/// Sends a batch of request records. They are written to the sockets now.
/// Returns 1 when some results are ready at once: call [dpgRun] soon.
/// Leaf calls never post to a port; only the helper thread of Rust does.
@Native<Int64 Function(Int64, Pointer<Uint8>, Int64)>(
  symbol: 'dpg_submit',
  isLeaf: true,
)
external int dpgSubmit(int link, Pointer<Uint8> data, int length);

/// Reads the answers that are there into [buffer]. Never waits.
@Native<Int64 Function(Int64, Pointer<Uint8>, Int64)>(
  symbol: 'dpg_run',
  isLeaf: true,
)
external int dpgRun(int link, Pointer<Uint8> buffer, int capacity);

/// For tests: the request and transaction timeout of a pool, in ms.
@Native<Void Function(Int64, Int64)>(symbol: 'dpg_set_timeout', isLeaf: true)
external void dpgSetTimeout(int pool, int ms);
