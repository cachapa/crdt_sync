# crdt_sync

A dart-native turnkey solution for painless network synchronization.

`crdt_sync` takes care of the network plumbing between your app and backend to build products that are:

* Offline-first: Apps primarily work with a local store independent of network connectivity.
* Real-time: When online, the local store immediately and continuously synchronizes with the backend.
* Efficient: Synchronization relies on delta changesets to optimize the amount of data sent over the wire.
* Portable: All communication happens over a single standard WebSocket making it compatible with most network configurations.

> ⚠️ This package is still under development and may not be stable. The API may break at any time.

## Usage

`crdt_sync` uses [sql_crdt](https://pub.dev/packages/sql_crdt) for data storage, so you'll need to instantiate one of [sqlite_crdt](https://pub.dev/packages/sqlite_crdt) or [postgres_crdt](https://pub.dev/packages/postgres_crdt).

You'll most likely want to store your server data in `PostgresCrdt`, and client data in `SqliteCrdt`, however the following sample code will just use a transient `SqliteCrdt` database for the sake of simplicity:

```dart
final crdt = await SqliteCrdt.openInMemory(…);
```

### Server

Start listening for connections:

```dart
listen(crdt, 8080);
```

Alternatively, you can use `upgrade()` to adopt an `HttpRequest`, or `CrdtSync.server()` to manage a `WebSocket` directly making it easy to integrate with [Shelf](https://pub.dev/packages/shelf) and other server frameworks.

See [tudo_server](https://github.com/cachapa/tudo_server) for a real world example.

### Client

Instantiate a `CrdtSyncClient` and order it to start connecting:

```dart
final client = CrdtSyncClient(crdt, Uri.parse('ws://localhost:8080'));
client.connect();
```

Once `connect()` is called, the client will continuously attempt to establish or resume a connection until it succeeds, or until `disconnect()` is called.

See the included [example](https://github.com/cachapa/crdt_sync/blob/master/example/example.dart) for a more complete solution, or See [tudo](https://github.com/cachapa/tudo) for a real world example.


## Features and bugs

Please file feature requests and bugs in the [issue tracker](https://github.com/cachapa/crdt_sync/issues).
