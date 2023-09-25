## 1.0.5

- Handle abnormal socket closures

## 1.0.4

- Do not throw exception on socket upgrade errors
- Added optional `onUpgradeError` to monitor upgrade failures

## 1.0.3

- Fix initial sync after handshake

## 1.0.2

- Update to latest `crdt` version

## 1.0.1

- Add optional `changesetBuilder` to allow for custom changeset generation

## 1.0.0

This version replaces the `sql_crdt` dependency with `crdt` thereby making it compatible with all its implementations.

Changes:
- Removed changeset generation methods since they're now implemented in `crdt`.
- Removed optional `changesetQueries` since it's now implemented in `sql_crdt`.
- Removed all SQL-related dependencies.

## 0.0.8

- Optional async `validateRecord` and `handshake` builders

## 0.0.7

- Fix error building full changesets

## 0.0.6

- Make `peerId` optional when building changesets

## 0.0.5

- Simplify API

## 0.0.4

- Major refactor

## 0.0.3

- Add remote nodeId to connection callbacks
- Improve changeset generation strategy
- Implement exponential backoff when reconnecting to server

## 0.0.2

- Fix error when reconnecting the client
- Fix last modified timestamps using the remote node id
- Add optional WebSocket heartbeat (on by default)
- Improve logic for handling connected clients

## 0.0.1+1

- Remove rxdart dependency

## 0.0.1

- Initial version
