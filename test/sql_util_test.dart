import 'package:crdt_sync/src/sql_util.dart';
import 'package:sqlite_crdt/sqlite_crdt.dart';
import 'package:test/test.dart';

void main() {
  group('Alter query', () {
    test('Simple query', () {
      final sql = SqlUtil.addChangesetClauses('test', 'SELECT * FROM test',
          exceptNodeId: 'node_id', afterHlc: Hlc.zero('node_id'));
      expect(sql,
          "SELECT * FROM test WHERE test.node_id != 'node_id' AND test.modified > '1970-01-01T00:00:00.000Z-0000-node_id'");
    });

    test('Simple query with where clause', () {
      final sql = SqlUtil.addChangesetClauses(
          'test', 'SELECT * FROM test WHERE a != ?1 and b = ?2',
          exceptNodeId: 'node_id', afterHlc: Hlc.zero('node_id'));
      expect(sql,
          "SELECT * FROM test WHERE test.node_id != 'node_id' AND test.modified > '1970-01-01T00:00:00.000Z-0000-node_id' AND a != ?1 AND b = ?2");
    });
  });
}
