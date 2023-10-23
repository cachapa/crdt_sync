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
/// [onUpgradeError] can be used to monitor connection->websocket upgrade errors.
///
/// See [CrdtSync.server] for a description of the remaining parameters.
Future<void> listen(
  Crdt crdt,
  int port, {
  Duration? pingInterval = defaultPingInterval,
  ServerHandshakeDataBuilder? handshakeDataBuilder,
  ChangesetBuilder? changesetBuilder,
  RecordValidator? validateRecord,
  ChangesetMapper? mapIncomingChangeset,
  void Function(HttpRequest request)? onConnecting,
  ServerOnConnect? onConnect,
  OnDisconnect? onDisconnect,
  OnChangeset? onChangesetReceived,
  OnChangeset? onChangesetSent,
  void Function(Object error, HttpRequest request)? onUpgradeError,
  bool verbose = false,
}) async {
  final server = await HttpServer.bind(InternetAddress.loopbackIPv4, port);
  if (verbose) print('Listening on localhost:${server.port}');

  await for (HttpRequest request in server) {
    onConnecting?.call(request);
    try {
      await upgrade(
        crdt,
        request,
        pingInterval: pingInterval,
        handshakeDataBuilder: handshakeDataBuilder,
        changesetBuilder: changesetBuilder,
        validateRecord: validateRecord,
        mapIncomingChangeset: mapIncomingChangeset,
        onConnect: onConnect,
        onDisconnect: onDisconnect,
        onChangesetReceived: onChangesetReceived,
        onChangesetSent: onChangesetSent,
        verbose: verbose,
      );
    } catch (e) {
      if (verbose) print(e);
      onUpgradeError?.call(e, request);
    }
  }
}

/// Upgrades an incoming connection to a WebSocket and synchronizes the provided
/// [crdt].
///
/// See [listen] for the parameter description.
Future<void> upgrade(
  Crdt crdt,
  HttpRequest request, {
  Duration? pingInterval = defaultPingInterval,
  ServerHandshakeDataBuilder? handshakeDataBuilder,
  ChangesetBuilder? changesetBuilder,
  RecordValidator? validateRecord,
  ChangesetMapper? mapIncomingChangeset,
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
    mapIncomingChangeset: mapIncomingChangeset,
    onConnect: (_, customData) => onConnect?.call(crdtSync, customData),
    onDisconnect: onDisconnect,
    onChangesetReceived: onChangesetReceived,
    onChangesetSent: onChangesetSent,
    verbose: verbose,
  );
}
