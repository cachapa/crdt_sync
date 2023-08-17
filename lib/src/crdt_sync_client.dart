import 'dart:async';
import 'dart:math';

import 'package:sql_crdt/sql_crdt.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'crdt_sync.dart';

const _minDelay = 2; // In seconds. Minimum is 2 because 1² = 1.
const _maxDelay = 10;

enum SocketState { disconnected, connecting, connected }

typedef ClientHandshakeDataBuilder = Map<String, dynamic>? Function();
typedef OnConnecting = void Function();

class CrdtSyncClient {
  final SqlCrdt crdt;
  final Uri uri;
  final ClientHandshakeDataBuilder? handshakeDataBuilder;
  final Map<String, Query>? changesetQueries;
  final RecordValidator? validateRecord;
  final OnConnecting? onConnecting;
  final OnConnect? onConnect;
  final OnDisconnect? onDisconnect;
  final OnChangeset? onChangesetReceived;
  final OnChangeset? onChangesetSent;
  final bool verbose;

  CrdtSync? _crdtSync;

  var _onlineMode = false;
  var _state = SocketState.disconnected;
  final _stateController = StreamController<SocketState>.broadcast();

  var _reconnectDelay = _minDelay; // in seconds
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
    this.changesetQueries,
    this.validateRecord,
    this.onConnecting,
    this.onConnect,
    this.onDisconnect,
    this.onChangesetReceived,
    this.onChangesetSent,
    this.verbose = false,
  })  : assert({'ws', 'wss'}.contains(uri.scheme)),
        assert(changesetQueries == null || changesetQueries.isNotEmpty);

  /// Start trying to connect to [uri].
  /// The client will try to connect every 10 seconds until it succeeds.
  void connect() async {
    if (_state != SocketState.disconnected) return;
    _onlineMode = true;
    _reconnectTimer?.cancel();

    _setState(SocketState.connecting);
    onConnecting?.call();

    try {
      final socket = WebSocketChannel.connect(uri);
      await socket.ready;
      _crdtSync = CrdtSync(
        crdt,
        socket,
        isClient: true,
        handshakeDataBuilder: (_, __) => handshakeDataBuilder?.call(),
        changesetQueries: changesetQueries,
        validateRecord: validateRecord,
        onConnect: (remoteNodeId, remoteInfo) {
          _reconnectDelay = _minDelay;
          _setState(SocketState.connected);
          onConnect?.call(remoteNodeId, remoteInfo);
        },
        onChangesetReceived: onChangesetReceived,
        onChangesetSent: onChangesetSent,
        onDisconnect: (remoteNodeId, code, reason) {
          _setState(SocketState.disconnected);
          onDisconnect?.call(remoteNodeId, code, reason);
          _crdtSync = null;
          _maybeReconnect();
        },
        verbose: verbose,
      );
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
    _reconnectDelay = _minDelay;

    await _crdtSync?.close(code, reason);
    _setState(SocketState.disconnected);
  }

  void _maybeReconnect() {
    if (_onlineMode) {
      _reconnectTimer =
          Timer(Duration(seconds: _reconnectDelay), () => connect());
      _log('Reconnecting in ${_reconnectDelay}s…');
      _reconnectDelay = min(_reconnectDelay * 2, _maxDelay);
    }
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
