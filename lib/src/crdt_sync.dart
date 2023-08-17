import 'dart:async';

import 'package:sql_crdt/sql_crdt.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'sql_util.dart';
import 'sync_socket.dart';

typedef Query = (String sql, List<Object?> args);

typedef RecordValidator = bool Function(
    String table, Map<String, dynamic> record);
typedef OnChangeset = void Function(
    String nodeId, Map<String, int> recordCounts);
typedef OnConnect = void Function(
    String peerId, Map<String, dynamic>? customData);
typedef OnDisconnect = void Function(String peerId, int? code, String? reason);

class CrdtSync {
  final bool isClient;
  final SqlCrdt crdt;

  // TODO variations for client and server
  final HandshakeDataBuilder? handshakeDataBuilder;
  final Map<String, Query> changesetQueries;
  final RecordValidator? validateRecord;
  final OnConnect? onConnect;
  final OnDisconnect? onDisconnect;
  final OnChangeset? onChangesetReceived;
  final OnChangeset? onChangesetSent;
  final bool verbose;

  late final SyncSocket _syncSocket;
  String? peerId;

  CrdtSync(
    this.crdt,
    WebSocketChannel webSocket, {
    required this.isClient,
    this.handshakeDataBuilder,
    Map<String, Query>? changesetQueries,
    this.validateRecord,
    this.onConnect,
    this.onDisconnect,
    this.onChangesetReceived,
    this.onChangesetSent,
    this.verbose = false,
  }) : changesetQueries = changesetQueries ?? {} {
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
        if (peerId != null) onDisconnect?.call(peerId!, code, reason);
      },
      onChangeset: _mergeChangeset,
      verbose: verbose,
    );

    try {
      final handshake = await _performHandshake();
      peerId = handshake.nodeId;
      onConnect?.call(peerId!, handshake.data);

      // Monitor for changes and send them immediately
      localSubscription = crdt.onTablesChanged
          // Filter out unspecified tables
          .map((e) =>
              (hlc: e.hlc, tables: tableSet.intersection(e.tables.toSet())))
          .where((e) => e.tables.isNotEmpty)
          .asyncMap((e) => getChangeset(
                crdt,
                changesetQueries.whereKey((k) => e.tables.contains(k)),
                isClient: isClient,
                peerId: peerId!,
                atHlc: e.hlc,
              ))
          // .listen(_sendChangeset);
          .listen((event) {
        _sendChangeset(event);
      });

      // Send changeset since last sync.
      // This is done after monitoring to prevent losing changes that happen
      // exactly between both calls.
      await getChangeset(
        crdt,
        changesetQueries,
        isClient: isClient,
        peerId: peerId!,
        afterHlc: handshake.lastModified,
      ).then(_sendChangeset);
    } catch (e) {
      _log('$e');
    }
  }

  Future<void> close([int? code, String? reason]) =>
      _syncSocket.close(code, reason);

  Future<Handshake> _performHandshake() async {
    if (isClient) {
      // Introduce ourselves
      _syncSocket.sendHandshake(
        crdt.nodeId,
        await crdt.lastModified(excludeNodeId: crdt.nodeId),
        handshakeDataBuilder?.call(null, null),
      );
      return await _syncSocket.receiveHandshake();
    } else {
      // A good client always introduces itself first
      final handshake = await _syncSocket.receiveHandshake();
      _syncSocket.sendHandshake(
        crdt.nodeId,
        await crdt.lastModified(onlyNodeId: handshake.nodeId),
        handshakeDataBuilder?.call(handshake.nodeId, handshake.data),
      );
      return handshake;
    }
  }

  void _sendChangeset(CrdtChangeset changeset) {
    if (changeset.isEmpty) return;
    onChangesetSent?.call(
        peerId!, changeset.map((key, value) => MapEntry(key, value.length)));
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
        peerId!, changeset.map((key, value) => MapEntry(key, value.length)));
    crdt.merge(changeset);
  }

  static Future<CrdtChangeset> getChangeset(
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
