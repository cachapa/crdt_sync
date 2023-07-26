import 'dart:async';

import 'package:crdt_sync/crdt_sync.dart';
import 'package:crdt_sync/src/sync_socket.dart';
import 'package:sql_crdt/sql_crdt.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'crdt_sync_server_locator.dart'
    if (dart.library.io) 'crdt_sync_server_io.dart';

typedef ServerHandshakeDataBuilder = Map<String, dynamic>? Function(
    String remoteNodeId, Map<String, dynamic>? remoteData);
typedef OnConnection = void Function(dynamic request);

abstract class CrdtSyncServer {
  SqlCrdt get crdt;

  bool get verbose;

  /// Get the number of connected clients.
  int get clientCount;

  /// A server that automatically synchronizes local changes with a remote
  /// client and vice-versa.
  ///
  /// Set [verbose] to true to spam your output with raw payloads.
  factory CrdtSyncServer(SqlCrdt crdt, {bool verbose = false}) =>
      getPlatformCrdtSyncServer(crdt, verbose);

  /// Opens an HTTP socket and starts listening for incoming connections on the
  /// specified [port].
  ///
  /// Use [handshakeDataBuilder] if you need to send connection metadata on the
  /// first frame. This can be useful to send server identifiers, or
  /// verification tokens.
  ///
  /// Use [tables] if you want to specify which tables to be synchronized.
  /// Defaults to all tables in the database.
  ///
  /// By default, [CrdtSyncServer] monitors all tables in the supplied [crdt].
  /// [queryBuilder] can be used if more complex use cases are needed, but be
  /// sure to use the supplied [lastModified] and [remoteNodeId] parameters to
  /// avoid generating larger than necessary datasets, or leaking data.
  /// Note that most servers will want to filter only the data that's relevant
  /// to the specific client.
  /// Return [null] to use the default query for that table.
  ///
  /// If implemented, [validateRecord] will be called for each incoming record.
  /// Returning false will cause that record to not be merged in the local
  /// database. This can be used for low-trust environments to e.g. avoid
  /// a user writing into tables it should not have access to.
  ///
  /// The [onConnection], [onConnect] and [onDisconnect] callbacks can be used
  /// to monitor the connection state.
  ///
  /// [onChangesetReceived] and [onChangesetSent] can be used to log the
  /// respective data transfers. This can be useful to identify data handling
  /// inefficiencies.
  Future<void> listen(
    int port, {
    ServerHandshakeDataBuilder? handshakeDataBuilder,
    Iterable<String>? tables,
    QueryBuilder? queryBuilder,
    RecordValidator? validateRecord,
    OnConnection? onConnecting,
    OnConnect? onConnect,
    OnDisconnect? onDisconnect,
    OnChangeset? onChangesetReceived,
    OnChangeset? onChangesetSent,
  });

  /// Takes an incoming [HttpRequest] and attempts to upgrade it to a
  /// [WebSocket] connection to start synchronizing with a [CrdtSyncClient].
  ///
  /// See [listen] for a description of the remaining parameters.
  Future<void> handleRequest(
    dynamic request, {
    ServerHandshakeDataBuilder? handshakeDataBuilder,
    Iterable<String>? tables,
    QueryBuilder? queryBuilder,
    RecordValidator? validateRecord,
    OnConnection? onConnecting,
    OnConnect? onConnect,
    OnDisconnect? onDisconnect,
    OnChangeset? onChangesetReceived,
    OnChangeset? onChangesetSent,
  });

  /// Takes an established [WebSocket] connection to start synchronizing with a
  /// [CrdtSyncClient].
  ///
  /// See [listen] for a description of the remaining parameters.
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
  });

  /// Forcefully disconnect a connected client identified by [nodeId].
  /// You can supply an optional [code] and [reason] which will be forwarded to
  /// the client.
  ///
  /// See https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent/code for
  /// a list of permissible codes.
  Future<void> disconnect(String nodeId, [int? code, String? reason]);
}
