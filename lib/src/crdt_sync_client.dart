import 'dart:async';

import 'package:crdt_sync/src/sync_socket.dart';
import 'package:sql_crdt/sql_crdt.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

enum SocketState { disconnected, connecting, connected }

typedef ClientHandshakeDataBuilder = Map<String, dynamic>? Function();
typedef OnConnecting = void Function();

const _reconnectDuration = Duration(seconds: 10);

class CrdtSyncClient {
  final SqlCrdt crdt;
  final Uri uri;
  final ClientHandshakeDataBuilder? handshakeDataBuilder;
  final Iterable<String>? tables;
  final QueryBuilder? queryBuilder;
  final RecordValidator? validateRecord;
  final OnConnecting? onConnecting;
  final OnConnect? onConnect;
  final OnDisconnect? onDisconnect;
  final OnChangeset? onChangesetReceived;
  final OnChangeset? onChangesetSent;
  final bool verbose;

  late final Set<String> _tableSet;
  var _onlineMode = false;
  SyncSocket? _syncSocket;
  var _state = SocketState.disconnected;
  final _stateController = StreamController<SocketState>.broadcast();

  Timer? _reconnectTimer;

  /// Get the current socket state.
  SocketState get state => _state;

  /// Stream socket state changes.
  Stream<SocketState> get watchState => _stateController.stream;

  /// A client that automatically synchronizes local changes with a remote
  /// server and vice-versa. It will manage the connection automatically, trying
  /// to connect or restore the connection until [disconnect] is called.
  ///
  /// Use [handshakeDataBuilder] if you need to send connection metadata on the
  /// first frame. This can be useful to send client identifiers, or auth
  /// tokens.
  ///
  /// Use [tables] if you want to specify which tables to be synchronized.
  /// Defaults to all tables in the database. Cannot be empty.
  ///
  /// By default, [CrdtSyncClient] monitors [tables] in the supplied [crdt].
  /// [queryBuilder] can be used if more complex use cases are needed, but be
  /// sure to use the supplied [lastModified] and [remoteNodeId] parameters to
  /// avoid generating larger than necessary datasets, or leaking data.
  /// Return [null] to use the default query for that table.
  ///
  /// If implemented, [validateRecord] will be called for each incoming record.
  /// Returning false will cause that record to not be merged in the local
  /// database. This can be used for low-trust environments to e.g. avoid
  /// a user writing into tables it should not have access to.
  ///
  /// The [onConnecting], [onConnect] and [onDisconnect] callbacks can be used
  /// to monitor the connection state. See also [state] and [watchState].
  ///
  /// [onChangesetReceived] and [onChangesetSent] can be used to log the
  /// respective data transfers. This can be useful to identify data handling
  /// inefficiencies.
  ///
  /// Set [verbose] to true to spam your output with raw payloads.
  CrdtSyncClient(
    this.crdt,
    this.uri, {
    this.handshakeDataBuilder,
    this.tables,
    this.queryBuilder,
    this.validateRecord,
    this.onConnecting,
    this.onConnect,
    this.onDisconnect,
    this.onChangesetReceived,
    this.onChangesetSent,
    this.verbose = false,
  })  : assert({'ws', 'wss'}.contains(uri.scheme)),
        assert(tables == null || tables.isNotEmpty);

  /// Start trying to connect to [uri].
  /// The client will try to connect every 10 seconds until it succeeds.
  void connect() async {
    if (_state != SocketState.disconnected) return;
    _onlineMode = true;
    _reconnectTimer?.cancel();

    _setState(SocketState.connecting);
    onConnecting?.call();

    StreamSubscription? localSubscription;
    Handshake? handshake;
    _tableSet = (tables ?? await crdt.allTables).toSet();

    try {
      final socket = WebSocketChannel.connect(uri);
      await socket.ready;
      _syncSocket = SyncSocket(
        crdt,
        socket,
        validateRecord: validateRecord,
        onConnect: (remoteNodeId, remoteInfo) {
          _setState(SocketState.connected);
          onConnect?.call(remoteNodeId, remoteInfo);
        },
        onChangesetReceived: onChangesetReceived,
        onChangesetSent: onChangesetSent,
        onDisconnect: (remoteNodeId, code, reason) {
          localSubscription?.cancel();
          _setState(SocketState.disconnected);
          onDisconnect?.call(remoteNodeId, code, reason);
          _syncSocket = null;
          _maybeReconnect();
        },
        verbose: verbose,
      );

      // Introduce ourselves
      _syncSocket!.sendHandshake(
          await crdt.lastModified(excludeNodeId: crdt.nodeId),
          handshakeDataBuilder?.call());
      handshake = await _syncSocket!.awaitHandshake();

      // Monitor for changes and send them immediately
      var lastUpdate = crdt.canonicalTime;
      localSubscription = crdt.onTablesChanged.asyncMap((e) async {
        // Filter out unexpected tables
        final allowedTables = _tableSet.intersection(e.tables.toSet());
        final changeset = await _getChangeset(allowedTables, lastUpdate);
        lastUpdate = e.hlc;
        return changeset;
      }).listen(_syncSocket!.sendChangeset);

      // Send changeset since last sync.
      // This is done after monitoring to prevent losing changes that happen
      // exactly between both calls.
      await _getChangeset(_tableSet, handshake.lastModified)
          .then(_syncSocket!.sendChangeset);
    } catch (e) {
      _log('$e');
      _setState(SocketState.disconnected);
      _maybeReconnect();
    }
  }

  /// Disconnect from the server, and stop attempting to reconnect.
  Future<void> disconnect([int? code, String? reason]) async {
    if (!_onlineMode) return;
    _onlineMode = false;
    _reconnectTimer?.cancel();

    await _syncSocket?.close(code, reason);
  }

  void _maybeReconnect() {
    if (_onlineMode) {
      _reconnectTimer = Timer(_reconnectDuration, () => connect());
      _log('Reconnecting in ${_reconnectDuration.inSeconds}s…');
    }
  }

  Future<Map<String, List<Map<String, Object?>>>> _getChangeset(
          Iterable<String> tables, Hlc hlc) async =>
      {
        for (final table in tables) table: await _getTableChangeset(table, hlc),
      }..removeWhere((_, records) => records.isEmpty);

  Future<List<Map<String, Object?>>> _getTableChangeset(String table, Hlc hlc) {
    final (sql, args) = queryBuilder?.call(table, hlc, crdt.nodeId) ??
        (
          'SELECT * FROM $table '
              'WHERE node_id = ?1 '
              'AND modified > ?2',
          [crdt.nodeId, hlc]
        );
    return crdt.query(sql, args);
  }

  void _setState(SocketState state) {
    if (_state == state) return;
    _state = state;
    _stateController.add(state);
  }

  void _log(String msg) {
    if (verbose) print(msg);
  }
}
