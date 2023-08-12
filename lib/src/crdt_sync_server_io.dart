import 'dart:async';
import 'dart:io';

import 'package:sql_crdt/sql_crdt.dart';
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'crdt_sync_server.dart';
import 'sql_util.dart';
import 'sync_socket.dart';

CrdtSyncServer getPlatformCrdtSyncServer(SqlCrdt crdt, bool verbose) =>
    CrdtSyncServerIo(crdt, verbose: verbose);

class CrdtSyncServerIo implements CrdtSyncServer {
  @override
  final SqlCrdt crdt;
  @override
  final bool verbose;

  final _connections = <SyncSocket>{};

  @override
  int get clientCount => _connections.length;

  CrdtSyncServerIo(
    this.crdt, {
    this.verbose = false,
  });

  @override
  Future<void> listen(
    int port, {
    Duration? pingInterval = defaultPingInterval,
    ServerHandshakeDataBuilder? handshakeDataBuilder,
    Map<String, Query>? changesetQueries,
    RecordValidator? validateRecord,
    OnConnection? onConnecting,
    OnConnect? onConnect,
    OnDisconnect? onDisconnect,
    OnChangeset? onChangesetReceived,
    OnChangeset? onChangesetSent,
  }) async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, port);
    _log('Listening on localhost:${server.port}');

    await for (HttpRequest request in server) {
      await handleRequest(
        request,
        pingInterval: pingInterval,
        handshakeDataBuilder: handshakeDataBuilder,
        changesetQueries: changesetQueries,
        validateRecord: validateRecord,
        onConnecting: onConnecting,
        onConnect: onConnect,
        onDisconnect: onDisconnect,
        onChangesetReceived: onChangesetReceived,
        onChangesetSent: onChangesetSent,
      );
    }
  }

  @override
  Future<void> handleRequest(
    dynamic request, {
    Duration? pingInterval = defaultPingInterval,
    ServerHandshakeDataBuilder? handshakeDataBuilder,
    Map<String, Query>? changesetQueries,
    RecordValidator? validateRecord,
    OnConnection? onConnecting,
    OnConnect? onConnect,
    OnDisconnect? onDisconnect,
    OnChangeset? onChangesetReceived,
    OnChangeset? onChangesetSent,
  }) async {
    assert(request is HttpRequest);

    onConnecting?.call(request);
    final socket =
        IOWebSocketChannel(await WebSocketTransformer.upgrade(request)
          ..pingInterval = pingInterval);
    await handle(
      socket,
      handshakeDataBuilder: handshakeDataBuilder,
      changesetQueries: changesetQueries,
      validateRecord: validateRecord,
      onConnect: onConnect,
      onChangesetReceived: onChangesetReceived,
      onChangesetSent: onChangesetSent,
      onDisconnect: onDisconnect,
    );
  }

  @override
  Future<void> handle(
    WebSocketChannel socket, {
    ServerHandshakeDataBuilder? handshakeDataBuilder,
    Map<String, Query>? changesetQueries,
    RecordValidator? validateRecord,
    OnConnect? onConnect,
    OnDisconnect? onDisconnect,
    OnChangeset? onChangesetReceived,
    OnChangeset? onChangesetSent,
  }) async {
    StreamSubscription? localSubscription;

    late final SyncSocket syncSocket;
    syncSocket = SyncSocket(
      crdt,
      socket,
      validateRecord: validateRecord,
      onConnect: (remoteNodeId, remoteInfo) {
        _connections.add(syncSocket);
        onConnect?.call(remoteNodeId, remoteInfo);
      },
      onDisconnect: (remoteNodeId, code, reason) {
        localSubscription?.cancel();
        _connections.remove(syncSocket);
        onDisconnect?.call(remoteNodeId, code, reason);
      },
      onChangesetReceived: onChangesetReceived,
      onChangesetSent: onChangesetSent,
      verbose: verbose,
    );

    final tableSet = (changesetQueries?.keys ?? await crdt.allTables).toSet();

    try {
      // A good client always introduces itself first
      final hs = await syncSocket.awaitHandshake();

      syncSocket.sendHandshake(
        await crdt.lastModified(onlyNodeId: hs.nodeId),
        handshakeDataBuilder?.call(hs.nodeId, hs.info),
      );

      // Monitor for changes and send them immediately
      localSubscription = crdt.onTablesChanged
          // Filter out unspecified tables
          .map((e) =>
              (hlc: e.hlc, tables: tableSet.intersection(e.tables.toSet())))
          .where((e) => e.tables.isNotEmpty)
          .asyncMap((e) =>
              _getChangeset(changesetQueries, e.tables, e.hlc, hs.nodeId))
          .listen(syncSocket.sendChangeset);

      // Send changeset since last sync.
      // This is done after monitoring to prevent losing changes that happen
      // exactly between both calls.
      await _getChangeset(
              changesetQueries, tableSet, hs.lastModified, hs.nodeId, true)
          .then(syncSocket.sendChangeset);
    } catch (e) {
      _log('$e');
    }
  }

  @override
  Future<void> disconnect(String nodeId, [int? code, String? reason]) async {
    await Future.wait(_connections
        .where((e) => e.nodeId == nodeId)
        .map((e) => e.close(code, reason)));
  }

  Future<Map<String, List<Map<String, Object?>>>> _getChangeset(
          Map<String, Query>? changesetQueries,
          Iterable<String> tables,
          Hlc hlc,
          String remoteNodeId,
          [bool afterHlc = false]) async =>
      {
        for (final table in tables)
          table: await _getTableChangeset(
              changesetQueries, table, hlc, remoteNodeId, afterHlc),
      }..removeWhere((_, records) => records.isEmpty);

  Future<List<Map<String, Object?>>> _getTableChangeset(
    Map<String, Query>? changesetQueries,
    String table,
    Hlc hlc,
    String remoteNodeId,
    bool afterHlc,
  ) {
    final (sql, args) =
        changesetQueries?[table] ?? ('SELECT * FROM $table', []);
    return crdt.query(
      SqlUtil.addChangesetClauses(
        sql,
        exceptNodeId: remoteNodeId,
        onHlc: afterHlc ? null : hlc,
        afterHlc: afterHlc ? hlc : null,
      ),
      args,
    );
  }

  void _log(String msg) {
    if (verbose) print(msg);
  }
}
