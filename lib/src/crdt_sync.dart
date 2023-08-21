import 'dart:async';

import 'package:sql_crdt/sql_crdt.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'sql_util.dart';
import 'sync_socket.dart';

typedef Query = (String sql, List<Object?> args);

typedef ClientHandshakeDataBuilder = Object? Function();
typedef ServerHandshakeDataBuilder = Object? Function(
    String peerId, Object? peerData);
typedef RecordValidator = bool Function(
    String table, Map<String, dynamic> record);
typedef OnChangeset = void Function(
    String nodeId, Map<String, int> recordCounts);
typedef OnConnect = void Function(String peerId, Object? customData);
typedef OnDisconnect = void Function(String peerId, int? code, String? reason);

class CrdtSync {
  final bool isClient;
  final SqlCrdt crdt;

  final ClientHandshakeDataBuilder? clientHandshakeDataBuilder;
  final ServerHandshakeDataBuilder? serverHandshakeDataBuilder;
  final Map<String, Query> changesetQueries;
  final RecordValidator? validateRecord;
  final OnConnect? onConnect;
  final OnDisconnect? onDisconnect;
  final OnChangeset? onChangesetReceived;
  final OnChangeset? onChangesetSent;
  final bool verbose;

  late final SyncSocket _syncSocket;
  String? _peerId;

  /// Represents the nodeId from the remote peer connected to this socket.
  String? get peerId => _peerId;

  /// Takes an established [WebSocket] connection to start synchronizing the
  /// supplied [crdt] with a remote CrdtSync instance.
  ///
  /// Use [handshakeDataBuilder] to send connection metadata on the first frame.
  /// This can be useful to send server identifiers, or verification tokens.
  ///
  /// Use [changesetQueries] if you want to specify a custom query to generate
  /// changesets.
  /// Defaults to a simple `SELECT *` for all tables in the database.
  ///
  /// If implemented, [validateRecord] will be called for each incoming record.
  /// Returning false prevents that record from being merged into the local
  /// database. This can be used for low-trust environments to e.g. avoid
  /// a user writing into tables it should not have access to.
  ///
  /// The [onConnect] and [onDisconnect] callbacks can be used to monitor the
  /// connection state.
  ///
  /// [onChangesetReceived] and [onChangesetSent] can be used to log the
  /// respective data transfers. This can be useful to identify data handling
  /// inefficiencies.
  ///
  /// Set [verbose] to true to spam your output with raw record payloads.
  CrdtSync.client(
    SqlCrdt crdt,
    WebSocketChannel webSocket, {
    ClientHandshakeDataBuilder? handshakeDataBuilder,
    Map<String, Query>? changesetQueries,
    RecordValidator? validateRecord,
    OnConnect? onConnect,
    OnDisconnect? onDisconnect,
    OnChangeset? onChangesetReceived,
    OnChangeset? onChangesetSent,
    bool verbose = false,
  }) : this._(
          crdt,
          webSocket,
          isClient: true,
          clientHandshakeDataBuilder: handshakeDataBuilder,
          changesetQueries: changesetQueries,
          validateRecord: validateRecord,
          onConnect: onConnect,
          onDisconnect: onDisconnect,
          onChangesetReceived: onChangesetReceived,
          onChangesetSent: onChangesetSent,
          verbose: verbose,
        );

  /// Takes an established [WebSocket] connection to start synchronizing with
  /// another CrdtSync socket.
  ///
  /// It's recommended that the supplied [socket] has a ping interval set to
  /// avoid stale connections. This can be done in the parent framework, e.g.
  /// by setting [pingInterval] in shelf_web_socket's [webSocketHandler].
  ///
  /// See [CrdtSync.client] for a description of the remaining parameters.
  CrdtSync.server(
    SqlCrdt crdt,
    WebSocketChannel webSocket, {
    ServerHandshakeDataBuilder? handshakeDataBuilder,
    Map<String, Query>? changesetQueries,
    RecordValidator? validateRecord,
    OnConnect? onConnect,
    OnDisconnect? onDisconnect,
    OnChangeset? onChangesetReceived,
    OnChangeset? onChangesetSent,
    bool verbose = false,
  }) : this._(
          crdt,
          webSocket,
          isClient: false,
          serverHandshakeDataBuilder: handshakeDataBuilder,
          changesetQueries: changesetQueries,
          validateRecord: validateRecord,
          onConnect: onConnect,
          onDisconnect: onDisconnect,
          onChangesetReceived: onChangesetReceived,
          onChangesetSent: onChangesetSent,
          verbose: verbose,
        );

  CrdtSync._(
    this.crdt,
    WebSocketChannel webSocket, {
    required this.isClient,
    this.clientHandshakeDataBuilder,
    this.serverHandshakeDataBuilder,
    required Map<String, Query>? changesetQueries,
    required this.validateRecord,
    required this.onConnect,
    required this.onDisconnect,
    required this.onChangesetReceived,
    required this.onChangesetSent,
    required this.verbose,
  })  : changesetQueries = changesetQueries ?? {},
        assert((isClient && serverHandshakeDataBuilder == null) ||
            (!isClient && clientHandshakeDataBuilder == null)) {
    _handle(webSocket);
  }

  Future<void> _handle(WebSocketChannel webSocket) async {
    // Initialize default changeset queries if necessary
    if (changesetQueries.isEmpty) {
      for (final table in await crdt.allTables) {
        changesetQueries[table] = ('SELECT * FROM $table', []);
      }
    }
    final tableSet = changesetQueries.keys.toSet();

    StreamSubscription? localSubscription;

    _syncSocket = SyncSocket(
      webSocket,
      crdt.nodeId,
      onDisconnect: (code, reason) {
        localSubscription?.cancel();
        if (_peerId != null) onDisconnect?.call(_peerId!, code, reason);
      },
      onChangeset: _mergeChangeset,
      verbose: verbose,
    );

    try {
      final handshake = await _performHandshake();
      _peerId = handshake.nodeId;
      onConnect?.call(_peerId!, handshake.data);

      // Monitor for changes and send them immediately
      localSubscription = crdt.onTablesChanged
          // Filter out unspecified tables
          .map((e) =>
              (hlc: e.hlc, tables: tableSet.intersection(e.tables.toSet())))
          .where((e) => e.tables.isNotEmpty)
          .asyncMap((e) => getChangeset(tables: e.tables, atHlc: e.hlc))
          .listen(_sendChangeset);

      // Send changeset since last sync.
      // This is done after monitoring to prevent losing changes that happen
      // exactly between both calls.
      final changeset = await getChangeset(afterHlc: handshake.lastModified);
      _sendChangeset(changeset);
    } catch (e) {
      _log('$e');
    }
  }

  /// Close the connection.
  ///
  /// Supply an optional [code] and [reason] to be forwarded to the peer.
  /// See https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent/code for
  /// a list of permissible codes.
  Future<void> close([int? code, String? reason]) =>
      _syncSocket.close(code, reason);

  Future<Handshake> _performHandshake() async {
    if (isClient) {
      // Introduce ourselves
      _syncSocket.sendHandshake(
        crdt.nodeId,
        await crdt.lastModified(excludeNodeId: crdt.nodeId),
        clientHandshakeDataBuilder?.call(),
      );
      return await _syncSocket.receiveHandshake();
    } else {
      // A good client always introduces itself first
      final handshake = await _syncSocket.receiveHandshake();
      _syncSocket.sendHandshake(
        crdt.nodeId,
        await crdt.lastModified(onlyNodeId: handshake.nodeId),
        serverHandshakeDataBuilder?.call(handshake.nodeId, handshake.data),
      );
      return handshake;
    }
  }

  void _sendChangeset(CrdtChangeset changeset) {
    if (changeset.isEmpty) return;
    onChangesetSent?.call(
        _peerId!, changeset.map((key, value) => MapEntry(key, value.length)));
    _syncSocket.sendChangeset(changeset);
  }

  void _mergeChangeset(CrdtChangeset changeset) {
    if (validateRecord != null) {
      changeset = changeset.map((table, records) => MapEntry(
          table,
          records
              .where((record) => validateRecord!.call(table, record))
              .toList()));
    }
    onChangesetReceived?.call(
        _peerId!, changeset.map((key, value) => MapEntry(key, value.length)));
    crdt.merge(changeset);
  }

  /// Async method to get a changeset using the provided [changesetQueries].
  /// Useful to perform synchronization outside of the normal WebSocket
  /// connection, e.g. using a REST call.
  ///
  /// Use [tables] to generate a subset of the entire changeset.
  Future<CrdtChangeset> getChangeset(
      {Iterable<String>? tables, Hlc? atHlc, Hlc? afterHlc}) {
    final queries = tables == null
        ? changesetQueries
        : changesetQueries.whereKey((k) => tables.contains(k));
    return buildChangeset(crdt, queries,
        isClient: isClient, peerId: _peerId!, atHlc: atHlc, afterHlc: afterHlc);
  }

  /// Utility method to generate a changeset without a [CrdtSync] instance.
  /// See [getChangeset].
  static Future<CrdtChangeset> buildChangeset(
    SqlCrdt crdt,
    Map<String, Query> changesetQueries, {
    required bool isClient,
    required String peerId,
    Hlc? atHlc,
    Hlc? afterHlc,
  }) async =>
      {
        for (final entry in changesetQueries.entries)
          entry.key: await _getTableChangeset(
            crdt,
            entry.key,
            entry.value.$1,
            entry.value.$2,
            isClient: isClient,
            peerId: peerId,
            atHlc: atHlc,
            afterHlc: afterHlc,
          ),
      }..removeWhere((_, records) => records.isEmpty);

  static Future<CrdtTableChangeset> _getTableChangeset(
      SqlCrdt crdt, String table, String sql, List<Object?> args,
      {required bool isClient,
      required String peerId,
      Hlc? atHlc,
      Hlc? afterHlc}) {
    assert((atHlc == null) ^ (afterHlc == null));

    return crdt.query(
      SqlUtil.addChangesetClauses(
        table,
        sql,
        onlyNodeId: isClient ? crdt.nodeId : null,
        exceptNodeId: isClient ? null : peerId,
        atHlc: atHlc,
        afterHlc: afterHlc,
      ),
      args,
    );
  }

  void _log(String msg) {
    if (verbose) print(msg);
  }
}

extension<K, V> on Map<K, V> {
  Map<K, V> whereKey(bool Function(K key) test) =>
      Map.of(this)..removeWhere((key, _) => !test(key));
}
