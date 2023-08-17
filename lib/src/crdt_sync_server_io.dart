import 'dart:async';
import 'dart:io';

import 'package:sql_crdt/sql_crdt.dart';
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'crdt_sync.dart';
import 'crdt_sync_server.dart';

CrdtSyncServer getPlatformCrdtSyncServer(
  SqlCrdt crdt,
  ServerHandshakeDataBuilder? handshakeDataBuilder,
  Map<String, Query>? changesetQueries,
  RecordValidator? validateRecord,
  OnConnect? onConnect,
  OnDisconnect? onDisconnect,
  OnChangeset? onChangesetReceived,
  OnChangeset? onChangesetSent,
  bool verbose,
) =>
    CrdtSyncServerIo(
      crdt,
      handshakeDataBuilder: handshakeDataBuilder,
      changesetQueries: changesetQueries,
      validateRecord: validateRecord,
      onConnect: onConnect,
      onDisconnect: onDisconnect,
      onChangesetReceived: onChangesetReceived,
      onChangesetSent: onChangesetSent,
      verbose: verbose,
    );

class CrdtSyncServerIo implements CrdtSyncServer {
  @override
  final SqlCrdt crdt;
  final ServerHandshakeDataBuilder? handshakeDataBuilder;
  final Map<String, Query>? changesetQueries;
  final RecordValidator? validateRecord;
  final OnConnect? onConnect;
  final OnDisconnect? onDisconnect;
  final OnChangeset? onChangesetReceived;
  final OnChangeset? onChangesetSent;
  @override
  final bool verbose;

  final _connections = <CrdtSync>{};

  @override
  int get clientCount => _connections.length;

  CrdtSyncServerIo(
    this.crdt, {
    this.handshakeDataBuilder,
    this.changesetQueries,
    this.validateRecord,
    this.onConnect,
    this.onDisconnect,
    this.onChangesetReceived,
    this.onChangesetSent,
    this.verbose = false,
  });

  @override
  Future<void> listen(
    int port, {
    Duration? pingInterval = defaultPingInterval,
    OnConnection? onConnecting,
  }) async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, port);
    _log('Listening on localhost:${server.port}');

    await for (HttpRequest request in server) {
      await handleRequest(
        request,
        pingInterval: pingInterval,
        onConnecting: onConnecting,
      );
    }
  }

  @override
  Future<void> handleRequest(
    dynamic request, {
    Duration? pingInterval = defaultPingInterval,
    OnConnection? onConnecting,
  }) async {
    assert(request is HttpRequest);

    onConnecting?.call(request);
    final socket =
        IOWebSocketChannel(await WebSocketTransformer.upgrade(request)
          ..pingInterval = pingInterval);
    handle(socket);
  }

  @override
  void handle(WebSocketChannel socket) {
    late final CrdtSync crdtSync;
    crdtSync = CrdtSync(
      crdt,
      socket,
      isClient: false,
      handshakeDataBuilder: (peerId, peerData) =>
          handshakeDataBuilder?.call(peerId!, peerData),
      changesetQueries: changesetQueries,
      validateRecord: validateRecord,
      onConnect: (peerId, remoteInfo) {
        _connections.add(crdtSync);
        onConnect?.call(peerId, remoteInfo);
      },
      onDisconnect: (peerId, code, reason) {
        _connections.remove(crdtSync);
        onDisconnect?.call(peerId, code, reason);
      },
      onChangesetReceived: onChangesetReceived,
      onChangesetSent: onChangesetSent,
      verbose: verbose,
    );
  }

  @override
  Future<void> disconnect(String peerId, [int? code, String? reason]) =>
      Future.wait(_connections
          .where((e) => e.peerId == peerId)
          .map((e) => e.close(code, reason)));

  @override
  Future<void> disconnectAll([int? code, String? reason]) =>
      Future.wait(_connections.map((e) => e.close(code, reason)));

  void _log(String msg) {
    if (verbose) print(msg);
  }
}
