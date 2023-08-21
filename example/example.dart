import 'dart:convert';
import 'dart:io';

import 'package:crdt_sync/crdt_sync.dart';
import 'package:crdt_sync/crdt_sync_server.dart';
import 'package:sqlite_crdt/sqlite_crdt.dart';
import 'package:uuid/uuid.dart';

CrdtSyncClient? crdtSyncClient;

/// This example implements a simple CLI chat app.
///
/// Note how the app doesn't need to be online before you can start typing.
/// A background process will continuously attempt to establish a connection,
/// and automatically sync all of the outstanding messages when it succeeds.
Future<void> main(List<String> args) async {
  final crdt = await SqliteCrdt.openInMemory(
    version: 1,
    onCreate: (crdt, version) async {
      await crdt.execute('''
        CREATE TABLE chat (
          id TEXT NOT NULL,
          author TEXT NOT NULL,
          message TEXT NOT NULL,
          PRIMARY KEY (id)
        )
      ''');
    },
  );

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
    crdtSyncClient = CrdtSyncClient(
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
    )..connect();
  }

  var lastModified = '';
  crdt
      .watch(
        'SELECT * FROM chat WHERE modified > ?1 AND node_id != ?2',
        () => [lastModified, crdt.nodeId],
      )
      .where((e) => e.isNotEmpty)
      .listen(
    (messages) {
      lastModified = messages.last['modified'] as String;
      for (final message in messages) {
        print('[${message['author']}] ${message['message']}');
      }
    },
  );

  // Can't use stdin.readLineSync() since it blocks the entire application
  stdin.transform(utf8.decoder).transform(const LineSplitter()).listen((line) =>
      crdt.execute('INSERT INTO chat (id, author, message) VALUES (?1, ?2, ?3)',
          [Uuid().v4(), author, line]));
}
