import 'dart:convert';
import 'dart:io';

import 'package:crdt/map_crdt.dart';
import 'package:crdt_sync/crdt_sync.dart';
import 'package:crdt_sync/crdt_sync_server.dart';
import 'package:uuid/uuid.dart';

/// This example implements a simple CLI chat app.
///
/// Note how the app doesn't need to be online before you can start typing.
/// A background process will continuously attempt to establish a connection,
/// and automatically sync all of the outstanding messages when it succeeds.
Future<void> main(List<String> args) async {
  final crdt = MapCrdt(['chat']);

  print('Welcome to CRDT Chat.');
  stdout.write('Your name: ');
  final author = stdin.readLineSync()?.trim();

  print('Hi $author. Type anything to send a message.');

  late String remoteAuthor;
  if (args.isEmpty) {
    // ignore: unawaited_futures
    listen(
      crdt,
      8080,
      handshakeDataBuilder: (_, __) => {'name': author},
      onConnecting: (request) => print(
          'Incoming connection from ${request.connectionInfo?.remoteAddress.address}'),
      onConnect: (crdtSync, peerData) {
        remoteAuthor = (peerData as Map)['name'];
        print('Client joined: $remoteAuthor');
      },
      onDisconnect: (peerId, code, reason) =>
          print('Client left: $remoteAuthor'),
      // verbose: true,
    );
  } else {
    // ignore: unawaited_futures
    CrdtSyncClient(
      crdt,
      Uri.parse('ws://${args.first}'),
      handshakeDataBuilder: () => {'name': author},
      onConnecting: () => print('Connecting…'),
      onConnect: (nodeId, info) {
        remoteAuthor = (info as Map)['name'];
        print('Connected to $remoteAuthor');
      },
      onDisconnect: (nodeId, code, reason) =>
          print('Disconnected from $remoteAuthor ($code $reason)'),
      // verbose: true,
    ).connect();
  }

  crdt.onTablesChanged.listen(
    (e) {
      final records = crdt.getChangeset(modifiedOn: e.hlc)['chat']!;
      for (final record in records) {
        final message = record['value'] as Map<String, dynamic>;
        print('[${message['author']}] ${message['line']}');
      }
    },
  );

  // Can't use stdin.readLineSync() since it blocks the entire application
  stdin.transform(utf8.decoder).transform(const LineSplitter()).listen((line) =>
      crdt.put('chat', Uuid().v4(), {'author': author, 'line': line}));
}
