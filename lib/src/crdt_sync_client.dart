import 'dart:async';
import 'dart:math';

import 'package:crdt/crdt.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'crdt_sync.dart';

const _minDelay = 2; // In seconds. Minimum is 2 because 1² = 1.
const _maxDelay = 10;

enum SocketState { disconnected, connecting, connected }

class CrdtSyncClient {
  final Crdt crdt;
  final Uri uri;
  final ClientHandshakeDataBuilder? handshakeDataBuilder;
  final RecordValidator? validateRecord;
  final void Function()? onConnecting;
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

  /// A client that automatically manages the connection state of an
  /// underlying [CrdtSync].
  ///
  /// Use [onConnecting] to monitor the state of outgoing connection attempts.
  ///
  /// See [CrdtSync.client] for a description of the remaining parameters.
  CrdtSyncClient(
    this.crdt,
    this.uri, {
    this.handshakeDataBuilder,
    this.validateRecord,
    this.onConnecting,
    this.onConnect,
    this.onDisconnect,
    this.onChangesetReceived,
    this.onChangesetSent,
    this.verbose = false,
  }) : assert({'ws', 'wss'}.contains(uri.scheme));

  /// Start trying to connect to [uri].
  /// The client will continuously try to connect using exponential backoff
  /// until it succeeds.
  void connect() async {
    if (_state != SocketState.disconnected) return;
    _onlineMode = true;
    _reconnectTimer?.cancel();

    _setState(SocketState.connecting);
    onConnecting?.call();

    try {
      final socket = WebSocketChannel.connect(uri);
      await socket.ready;
      _crdtSync = CrdtSync.client(
        crdt,
        socket,
        handshakeDataBuilder: handshakeDataBuilder,
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
