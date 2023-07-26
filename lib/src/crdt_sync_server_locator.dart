import 'package:sql_crdt/sql_crdt.dart';

import 'crdt_sync_server.dart';

CrdtSyncServer getPlatformCrdtSyncServer(SqlCrdt crdt, bool verbose) =>
    throw UnsupportedError('Unsupported platform');
