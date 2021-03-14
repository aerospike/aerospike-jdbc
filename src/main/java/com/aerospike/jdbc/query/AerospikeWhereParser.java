package com.aerospike.jdbc.query;

import com.aerospike.jdbc.model.OpType;
import com.aerospike.jdbc.model.WhereExpression;
import io.prestosql.sql.tree.*;

public class AerospikeWhereParser {

    private AerospikeWhereParser() {
    }

    public static WhereExpression parseExpression(Expression expression) {
        return (new AerospikeWhereParser.Formatter()).process(expression, null);
    }

    public static class Formatter extends AstVisitor<WhereExpression, Void> {

        public Formatter() {
        }

        protected WhereExpression visitNode(Node node, Void context) {
            throw new UnsupportedOperationException("visitNode");
        }

        protected WhereExpression visitRow(Row node, Void context) {
            throw new UnsupportedOperationException("visitRow");
        }

        protected WhereExpression visitExpression(Expression node, Void context) {
            throw new UnsupportedOperationException("visitExpression");
        }

        protected WhereExpression visitAtTimeZone(AtTimeZone node, Void context) {
            throw new UnsupportedOperationException("visitAtTimeZone");
        }

        protected WhereExpression visitCurrentUser(CurrentUser node, Void context) {
            throw new UnsupportedOperationException("visitCurrentUser");
        }

        protected WhereExpression visitCurrentPath(CurrentPath node, Void context) {
            throw new UnsupportedOperationException("visitCurrentPath");
        }

        protected WhereExpression visitFormat(Format node, Void context) {
            throw new UnsupportedOperationException("visitFormat");
        }

        protected WhereExpression visitCurrentTime(CurrentTime node, Void context) {
            throw new UnsupportedOperationException("visitCurrentTime");
        }

        protected WhereExpression visitExtract(Extract node, Void context) {
            throw new UnsupportedOperationException("visitExtract");
        }

        protected WhereExpression visitBooleanLiteral(BooleanLiteral node, Void context) {
            throw new UnsupportedOperationException("visitBooleanLiteral");
        }

        protected WhereExpression visitCharLiteral(CharLiteral node, Void context) {
            throw new UnsupportedOperationException("visitCharLiteral");
        }

        protected WhereExpression visitBinaryLiteral(BinaryLiteral node, Void context) {
            throw new UnsupportedOperationException("visitBinaryLiteral");
        }

        protected WhereExpression visitParameter(Parameter node, Void context) {
            throw new UnsupportedOperationException("visitParameter");
        }

        protected WhereExpression visitAllRows(AllRows node, Void context) {
            throw new UnsupportedOperationException("visitAllRows");
        }

        protected WhereExpression visitArrayConstructor(ArrayConstructor node, Void context) {
            throw new UnsupportedOperationException("visitArrayConstructor");
        }

        protected WhereExpression visitSubscriptExpression(SubscriptExpression node, Void context) {
            throw new UnsupportedOperationException("visitSubscriptExpression");
        }

        protected WhereExpression visitLongLiteral(LongLiteral node, Void context) {
            throw new UnsupportedOperationException("visitLongLiteral");
        }

        protected WhereExpression visitDoubleLiteral(DoubleLiteral node, Void context) {
            throw new UnsupportedOperationException("visitDoubleLiteral");
        }

        protected WhereExpression visitDecimalLiteral(DecimalLiteral node, Void context) {
            throw new UnsupportedOperationException("visitDecimalLiteral");
        }

        protected WhereExpression visitGenericLiteral(GenericLiteral node, Void context) {
            throw new UnsupportedOperationException("visitGenericLiteral");
        }

        protected WhereExpression visitTimeLiteral(TimeLiteral node, Void context) {
            throw new UnsupportedOperationException("visitTimeLiteral");
        }

        protected WhereExpression visitTimestampLiteral(TimestampLiteral node, Void context) {
            throw new UnsupportedOperationException("visitTimestampLiteral");
        }

        protected WhereExpression visitNullLiteral(NullLiteral node, Void context) {
            throw new UnsupportedOperationException("visitNullLiteral");
        }

        protected WhereExpression visitIntervalLiteral(IntervalLiteral node, Void context) {
            throw new UnsupportedOperationException("visitIntervalLiteral");
        }

        protected WhereExpression visitSubqueryExpression(SubqueryExpression node, Void context) {
            throw new UnsupportedOperationException("visitSubqueryExpression");
        }

        protected WhereExpression visitExists(ExistsPredicate node, Void context) {
            throw new UnsupportedOperationException("visitExists");
        }

        protected WhereExpression visitIdentifier(Identifier node, Void context) {
            throw new UnsupportedOperationException("visitIdentifier");
        }

        protected WhereExpression visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, Void context) {
            throw new UnsupportedOperationException("visitLambdaArgumentDeclaration");
        }

        protected WhereExpression visitSymbolReference(SymbolReference node, Void context) {
            throw new UnsupportedOperationException("visitSymbolReference");
        }

        protected WhereExpression visitDereferenceExpression(DereferenceExpression node, Void context) {
            throw new UnsupportedOperationException("visitDereferenceExpression");
        }

        public WhereExpression visitFieldReference(FieldReference node, Void context) {
            throw new UnsupportedOperationException("visitFieldReference");
        }

        protected WhereExpression visitFunctionCall(FunctionCall node, Void context) {
            throw new UnsupportedOperationException("visitFunctionCall");
        }

        protected WhereExpression visitLambdaExpression(LambdaExpression node, Void context) {
            throw new UnsupportedOperationException("visitLambdaExpression");
        }

        protected WhereExpression visitBindExpression(BindExpression node, Void context) {
            throw new UnsupportedOperationException("visitBindExpression");
        }

        protected WhereExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
            WhereExpression exp = new WhereExpression();
            exp.setOpType(OpType.fromOperator(node.getOperator().toString()));
            exp.append(process(node.getLeft()));
            exp.append(process(node.getRight()));
            return exp;
        }

        protected WhereExpression visitNotExpression(NotExpression node, Void context) {
            WhereExpression exp = new WhereExpression();
            exp.setOpType(OpType.NOT);
            exp.append(process(node.getValue()));
            return exp;
        }

        protected WhereExpression visitComparisonExpression(ComparisonExpression node, Void context) {
            return this.formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        protected WhereExpression visitIsNullPredicate(IsNullPredicate node, Void context) {
            return new WhereExpression(OpType.NULL, node.getValue().toString());
        }

        protected WhereExpression visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
            return new WhereExpression(OpType.NOT_NULL, node.getValue().toString());
        }

        protected WhereExpression visitNullIfExpression(NullIfExpression node, Void context) {
            throw new UnsupportedOperationException("visitNullIfExpression");
        }

        protected WhereExpression visitIfExpression(IfExpression node, Void context) {
            throw new UnsupportedOperationException("visitIfExpression");
        }

        protected WhereExpression visitTryExpression(TryExpression node, Void context) {
            throw new UnsupportedOperationException("visitTryExpression");
        }

        protected WhereExpression visitCoalesceExpression(CoalesceExpression node, Void context) {
            throw new UnsupportedOperationException("visitCoalesceExpression");
        }

        protected WhereExpression visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
            throw new UnsupportedOperationException("visitArithmeticUnary");
        }

        protected WhereExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
            throw new UnsupportedOperationException("visitArithmeticBinary");
        }

        protected WhereExpression visitLikePredicate(LikePredicate node, Void context) {
            return new WhereExpression(node.getValue().toString(), OpType.LIKE, node.getPattern().toString());
        }

        protected WhereExpression visitAllColumns(AllColumns node, Void context) {
            throw new UnsupportedOperationException("visitAllColumns");
        }

        public WhereExpression visitCast(Cast node, Void context) {
            throw new UnsupportedOperationException("visitCast");
        }

        protected WhereExpression visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
            throw new UnsupportedOperationException("visitSimpleCaseExpression");
        }

        protected WhereExpression visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
            throw new UnsupportedOperationException("visitSimpleCaseExpression");
        }

        protected WhereExpression visitWhenClause(WhenClause node, Void context) {
            throw new UnsupportedOperationException("visitWhenClause");
        }

        protected WhereExpression visitBetweenPredicate(BetweenPredicate node, Void context) {
            // TODO add support
//            return "(" + this.process(node.getValue(), context) + " BETWEEN " + this.process(node.getMin(), context)
//                    + " AND " + this.process(node.getMax(), context) + ")";
            throw new UnsupportedOperationException();
        }

        protected WhereExpression visitInPredicate(InPredicate node, Void context) {
            // TODO add support
//            return "(" + this.process(node.getValue(), context) + " IN " + this.process(node.getValueList(), context) + ")";
            throw new UnsupportedOperationException();
        }

        protected WhereExpression visitInListExpression(InListExpression node, Void context) {
            // TODO add support
//            return "(" + this.joinExpressions(node.getValues()) + ")";
            throw new UnsupportedOperationException();
        }

        public WhereExpression visitWindow(Window node, Void context) {
            throw new UnsupportedOperationException("visitWindow");
        }

        public WhereExpression visitWindowFrame(WindowFrame node, Void context) {
            throw new UnsupportedOperationException("visitWindowFrame");
        }

        public WhereExpression visitFrameBound(FrameBound node, Void context) {
            throw new UnsupportedOperationException("visitFrameBound");
        }

        protected WhereExpression visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Void context) {
            throw new UnsupportedOperationException("visitQuantifiedComparisonExpression");
        }

        protected WhereExpression visitGroupingOperation(GroupingOperation node, Void context) {
            throw new UnsupportedOperationException("visitGroupingOperation");
        }

        protected WhereExpression visitRowDataType(RowDataType node, Void context) {
            throw new UnsupportedOperationException("visitRowDataType");
        }

        protected WhereExpression visitRowField(RowDataType.Field node, Void context) {
            throw new UnsupportedOperationException("visitRowField");
        }

        protected WhereExpression visitGenericDataType(GenericDataType node, Void context) {
            throw new UnsupportedOperationException("visitGenericDataType");
        }

        protected WhereExpression visitTypeParameter(TypeParameter node, Void context) {
            return this.process(node.getValue(), context);
        }

        protected WhereExpression visitNumericTypeParameter(NumericParameter node, Void context) {
            throw new UnsupportedOperationException("visitNumericTypeParameter");
        }

        protected WhereExpression visitIntervalDataType(IntervalDayTimeDataType node, Void context) {
            throw new UnsupportedOperationException("visitIntervalDataType");
        }

        protected WhereExpression visitDateTimeType(DateTimeDataType node, Void context) {
            throw new UnsupportedOperationException("visitDateTimeType");
        }

        private WhereExpression formatBinaryExpression(String operator, Expression left, Expression right) {
            WhereExpression exp = new WhereExpression();
            exp.setOpType(OpType.fromOperator(operator));
            exp.setColumn(left.toString());
            exp.setValue(right.toString());
            return exp;
        }
    }
}
