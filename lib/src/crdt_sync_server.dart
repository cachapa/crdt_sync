import 'dart:async';
import 'dart:io';

import 'package:crdt/crdt.dart';
import 'package:web_socket_channel/io.dart';

import 'crdt_sync.dart';

const defaultPingInterval = Duration(seconds: 20);

typedef ServerOnConnect = void Function(CrdtSync crdtSync, Object? customData);

/// Opens an HTTP socket and starts listening for incoming connections on the
/// specified [port].
///
/// [pingInterval] defines the WebSocket heartbeat frequency and allows the
/// server to identify and release stale connections. This is highly
/// recommended since stale connections keep database subscriptions which
/// cause queries to be run on every change.
/// Defaults to 20 seconds, set to [null] to disable.
///
/// Use [onConnection] to monitor incoming HTTP connections.
///
/// See [CrdtSync.server] for a description of the remaining parameters.
Future<void> listen(
  Crdt crdt,
  int port, {
  Duration? pingInterval = defaultPingInterval,
  ServerHandshakeDataBuilder? handshakeDataBuilder,
  RecordValidator? validateRecord,
  void Function(HttpRequest request)? onConnecting,
  ServerOnConnect? onConnect,
  OnDisconnect? onDisconnect,
  OnChangeset? onChangesetReceived,
  OnChangeset? onChangesetSent,
  bool verbose = false,
}) async {
  final server = await HttpServer.bind(InternetAddress.loopbackIPv4, port);
  if (verbose) print('Listening on localhost:${server.port}');

  await for (HttpRequest request in server) {
    onConnecting?.call(request);
    await upgrade(
      crdt,
      request,
      pingInterval: pingInterval,
      handshakeDataBuilder: handshakeDataBuilder,
      validateRecord: validateRecord,
      onConnect: onConnect,
      onDisconnect: onDisconnect,
      onChangesetReceived: onChangesetReceived,
      onChangesetSent: onChangesetSent,
      verbose: verbose,
    );
  }
}

Future<void> upgrade(
  Crdt crdt,
  HttpRequest request, {
  Duration? pingInterval = defaultPingInterval,
  ServerHandshakeDataBuilder? handshakeDataBuilder,
  ChangesetBuilder? changesetBuilder,
  RecordValidator? validateRecord,
  ServerOnConnect? onConnect,
  OnDisconnect? onDisconnect,
  OnChangeset? onChangesetReceived,
  OnChangeset? onChangesetSent,
  bool verbose = false,
}) async {
  final webSocket =
      IOWebSocketChannel(await WebSocketTransformer.upgrade(request)
        ..pingInterval = pingInterval);
  late final CrdtSync crdtSync;
  crdtSync = CrdtSync.server(
    crdt,
    webSocket,
    handshakeDataBuilder: handshakeDataBuilder,
    changesetBuilder: changesetBuilder,
    validateRecord: validateRecord,
    onConnect: (_, customData) => onConnect?.call(crdtSync, customData),
    onDisconnect: onDisconnect,
    onChangesetReceived: onChangesetReceived,
    onChangesetSent: onChangesetSent,
    verbose: verbose,
  );
}
