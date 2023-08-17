import 'package:sql_crdt/sql_crdt.dart';

import 'crdt_sync.dart';
import 'crdt_sync_server.dart';

CrdtSyncServer getPlatformCrdtSyncServer(
  SqlCrdt crdt,
  ServerHandshakeDataBuilder? handshakeDataBuilder,
  Map<String, Query>? changesetQueries,
  RecordValidator? validateRecord,
  OnConnect? onConnect,
  OnDisconnect? onDisconnect,
  OnChangeset? onChangesetReceived,
  OnChangeset? onChangesetSent,
  bool verbose,
) =>
    throw UnsupportedError('Unsupported platform');
