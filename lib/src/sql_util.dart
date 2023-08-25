// ignore: depend_on_referenced_packages
import 'package:source_span/source_span.dart';
import 'package:sql_crdt/sql_crdt.dart';
import 'package:sqlparser/sqlparser.dart';
import 'package:sqlparser/utils/node_to_text.dart';

final _parser = SqlEngine();
// https://github.com/simolus3/drift/discussions/2560#discussioncomment-6709055
final _span = SourceFile.fromString('fake').span(0);

class SqlUtil {
  SqlUtil._();

  static String addChangesetClauses(
    String table,
    String sql, {
    String? onlyNodeId,
    String? exceptNodeId,
    Hlc? atHlc,
    Hlc? afterHlc,
  }) {
    assert(onlyNodeId == null || exceptNodeId == null);
    assert(atHlc == null || afterHlc == null);

    final statement = _parser.parse(sql).rootNode as SelectStatement;

    final clauses = [
      if (onlyNodeId != null)
        _createClause(table, 'node_id', TokenType.equal, onlyNodeId),
      if (exceptNodeId != null)
        _createClause(
            table, 'node_id', TokenType.exclamationEqual, exceptNodeId),
      if (atHlc != null)
        _createClause(table, 'modified', TokenType.equal, atHlc.toString()),
      if (afterHlc != null)
        _createClause(table, 'modified', TokenType.more, afterHlc.toString()),
      if (statement.where != null) statement.where!,
    ];

    if (clauses.isNotEmpty) {
      statement.where =
          clauses.reduce((left, right) => _joinClauses(left, right));
    }

    return statement.toSql();
  }

  static BinaryExpression _createClause(
          String table, String column, TokenType operator, String value) =>
      BinaryExpression(
        Reference(columnName: column),
        Token(operator, _span),
        StringLiteral(value),
      );

  static BinaryExpression _joinClauses(Expression left, Expression right) =>
      BinaryExpression(left, Token(TokenType.and, _span), right);
}
