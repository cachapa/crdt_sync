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
    String sql, {
    String? onlyNodeId,
    String? exceptNodeId,
    Hlc? onHlc,
    Hlc? afterHlc,
  }) {
    final statement = _parser.parse(sql).rootNode as SelectStatement;

    final nodeIdClause = onlyNodeId != null
        ? _createClause('node_id', TokenType.equal, onlyNodeId)
        : _createClause('node_id', TokenType.exclamationEqual, exceptNodeId!);
    final modifiedClause = onHlc != null
        ? _createClause('modified', TokenType.equal, onHlc.toString())
        : _createClause('modified', TokenType.more, afterHlc!.toString());
    final clauses = _joinClauses(nodeIdClause, modifiedClause);

    statement.where = statement.where != null
        ? _joinClauses(clauses, statement.where!)
        : clauses;
    return statement.toSql();
  }

  static BinaryExpression _createClause(
          String column, TokenType operator, String value) =>
      BinaryExpression(
        Reference(columnName: column),
        Token(operator, _span),
        StringLiteral(value),
      );

  static BinaryExpression _joinClauses(Expression left, Expression right) =>
      BinaryExpression(left, Token(TokenType.and, _span), right);
}
