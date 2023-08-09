import 'dart:async';
import 'dart:io';

import 'package:crdt_sync/src/sync_socket.dart';
import 'package:sql_crdt/sql_crdt.dart';
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'crdt_sync_server.dart';

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
    Iterable<String>? tables,
    QueryBuilder? queryBuilder,
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
        tables: tables,
        queryBuilder: queryBuilder,
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
    Iterable<String>? tables,
    QueryBuilder? queryBuilder,
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
      tables: tables,
      queryBuilder: queryBuilder,
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
    Iterable<String>? tables,
    QueryBuilder? queryBuilder,
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

    final tableSet = (tables ?? await crdt.allTables).toSet();

    try {
      // A good client always introduces itself first
      final hs = await syncSocket.awaitHandshake();

      syncSocket.sendHandshake(
        await crdt.lastModified(onlyNodeId: hs.nodeId),
        handshakeDataBuilder?.call(hs.nodeId, hs.info),
      );

      // Monitor for changes and send them immediately
      var lastUpdate = crdt.canonicalTime;
      localSubscription = crdt.onTablesChanged.asyncMap((e) async {
        // Filter out unspecified tables
        final allowedTables = tableSet.intersection(e.tables.toSet());
        final changeset = await _getChangeset(
            queryBuilder, allowedTables, lastUpdate, hs.nodeId);
        lastUpdate = e.hlc;
        return changeset;
      }).listen(syncSocket.sendChangeset);

      // Send changeset since last sync.
      // This is done after monitoring to prevent losing changes that happen
      // exactly between both calls.
      await _getChangeset(queryBuilder, tableSet, hs.lastModified, hs.nodeId)
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
    QueryBuilder? queryBuilder,
    Iterable<String> tables,
    Hlc hlc,
    String remoteNodeId,
  ) async =>
      {
        for (final table in tables)
          table:
              await _getTableChangeset(queryBuilder, table, hlc, remoteNodeId),
      }..removeWhere((_, records) => records.isEmpty);

  Future<List<Map<String, Object?>>> _getTableChangeset(
      QueryBuilder? queryBuilder, String table, Hlc hlc, String remoteNodeId) {
    final (sql, args) = queryBuilder?.call(table, hlc, remoteNodeId) ??
        (
          'SELECT * FROM $table '
              'WHERE node_id != ?1 '
              'AND modified > ?2',
          [remoteNodeId, hlc]
        );
    return crdt.query(sql, args);
  }

  void _log(String msg) {
    if (verbose) print(msg);
  }
}
