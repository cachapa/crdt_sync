import 'dart:async';
import 'dart:convert';

import 'package:sql_crdt/sql_crdt.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

typedef Handshake = ({
  String nodeId,
  Hlc lastModified,
  Map<String, dynamic>? info,
});

typedef QueryBuilder = (String query, List<Object?> args)? Function(
    String table, Hlc lastModified, String remoteNodeId);

typedef RecordValidator = bool Function(
    String table, Map<String, dynamic> record);
typedef OnConnect = void Function(
    String remoteNodeId, Map<String, dynamic>? remoteInfo);
typedef OnDisconnect = void Function(
    String remoteNodeId, int? code, String? reason);
typedef OnChangeset = void Function(Map<String, int> recordCounts);

class SyncSocket {
  final SqlCrdt crdt;
  final WebSocketChannel socket;
  final RecordValidator? validateRecord;
  final OnConnect? onConnect;
  final OnDisconnect onDisconnect;
  final OnChangeset? onChangesetReceived;
  final OnChangeset? onChangesetSent;
  final bool verbose;

  late final StreamSubscription _subscription;

  Handshake? handshake;
  final _handshakeCompleter = Completer<Handshake>();

  String? get nodeId => handshake?.nodeId;

  SyncSocket(
    this.crdt,
    this.socket, {
    required this.validateRecord,
    required this.onConnect,
    required this.onDisconnect,
    required this.onChangesetReceived,
    required this.onChangesetSent,
    required this.verbose,
  }) {
    _subscription = socket.stream.map((e) => jsonDecode(e)).listen(
      (message) async {
        _log('⬇️ $message');
        if (handshake == null) {
          // The first message is a handshake
          handshake = (
            nodeId: message['node_id'] as String,
            lastModified: Hlc.parse(message['last_modified'] as String)
                // Modified timestamps always use the local node id
                .apply(nodeId: crdt.nodeId),
            info: message['info'] as Map<String, dynamic>?
          );
          _handshakeCompleter.complete(handshake);
          onConnect?.call(handshake!.nodeId, handshake!.info);
        } else {
          // Merge into crdt
          final changeset = (message as Map<String, dynamic>)
              .map((table, records) => MapEntry(
                    table,
                    (records as List).cast<Map<String, dynamic>>().where(
                        (record) =>
                            validateRecord?.call(table, record) ?? true),
                  ));
          onChangesetReceived?.call(
              changeset.map((key, value) => MapEntry(key, value.length)));
          await crdt.merge(changeset);
        }
      },
      onError: (e) => _log('$e'),
      onDone: close,
    );
  }

  void _send(Map<String, Object?> data) {
    _log('⬆️ $data');
    socket.sink.add(jsonEncode(data));
  }

  Future<Handshake> awaitHandshake() => _handshakeCompleter.future;

  void sendHandshake(Hlc lastModified, dynamic info) => _send({
        'node_id': crdt.nodeId,
        'last_modified': lastModified,
        'info': info,
      });

  void sendChangeset(Map<String, List<Map<String, Object?>>> changeset) {
    if (changeset.isEmpty) return;
    onChangesetSent
        ?.call(changeset.map((key, value) => MapEntry(key, value.length)));
    _send(changeset);
  }

  Future<void> close([int? code, String? reason]) async {
    await Future.wait([
      _subscription.cancel(),
      socket.sink.close(code, reason),
    ]);

    // Notify only when the connection was fully established (w/ handshake)
    if (handshake != null) {
      onDisconnect.call(
          handshake!.nodeId, socket.closeCode, socket.closeReason);
    }
  }

  void _log(String msg) {
    if (verbose) print(msg);
  }
}
