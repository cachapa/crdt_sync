import 'dart:async';
import 'dart:convert';

import 'package:crdt/crdt.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

typedef Handshake = ({
  String nodeId,
  Hlc lastModified,
  Map<String, dynamic>? data,
});

class SyncSocket {
  final WebSocketChannel socket;
  final void Function(int? code, String? reason) onDisconnect;
  final void Function(CrdtChangeset changeset) onChangeset;
  final bool verbose;

  late final StreamSubscription _subscription;

  final _handshakeCompleter = Completer<Handshake>();

  SyncSocket(
    this.socket,
    String localNodeId, {
    required this.onDisconnect,
    required this.onChangeset,
    required this.verbose,
  }) {
    _subscription = socket.stream.map((e) => jsonDecode(e)).listen(
      (message) async {
        _log('⬇️ $message');
        if (!_handshakeCompleter.isCompleted) {
          // The first message is a handshake
          _handshakeCompleter.complete((
            nodeId: message['node_id'] as String,
            // Modified timestamps always use the local node id
            lastModified: Hlc.parse(message['last_modified'] as String)
                .apply(nodeId: localNodeId),
            data: message['data'] as Map<String, dynamic>?
          ));
        } else {
          // Merge into crdt
          final changeset = (message as Map<String, dynamic>)
              // Cast payload to CrdtChangeset
              .map((table, records) => MapEntry(
                  table,
                  (records as List)
                      .cast<Map<String, dynamic>>()
                      // Parse Hlc
                      .map((e) => e.map((key, value) => MapEntry(
                          key, key == 'hlc' ? Hlc.parse(value) : value)))
                      .toList()));
          onChangeset(changeset);
        }
      },
      onError: (e) => _log('$e'),
      onDone: close,
    );
  }

  void _send(Map<String, Object?> data) {
    if (data.isEmpty) return;
    _log('⬆️ $data');
    socket.sink.add(jsonEncode(data));
  }

  Future<Handshake> receiveHandshake() => _handshakeCompleter.future;

  void sendHandshake(String nodeId, Hlc lastModified, Object? data) => _send({
        'node_id': nodeId,
        'last_modified': lastModified,
        'data': data,
      });

  void sendChangeset(CrdtChangeset changeset) => _send(changeset);

  Future<void> close([int? code, String? reason]) async {
    await Future.wait([
      _subscription.cancel(),
      socket.sink.close(code, reason),
    ]);

    onDisconnect(socket.closeCode, socket.closeReason);
  }

  void _log(String msg) {
    if (verbose) print(msg);
  }
}
