package com.aerospike.jdbc.query;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.prestosql.sql.SqlFormatter;
import io.prestosql.sql.tree.*;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class AerospikeExpressionParser {

    private static final ThreadLocal<DecimalFormat> doubleFormatter = ThreadLocal.withInitial(() ->
            new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));

    private AerospikeExpressionParser() {
    }

    public static String formatExpression(Expression expression) {
        return (new Formatter()).process(expression, null);
    }

    private static String formatIdentifier(String s) {
        return '"' + s.replace("\"", "\"\"") + '"';
    }

    static String formatStringLiteral(String s) {
        s = s.replace("'", "''");
        if (CharMatcher.inRange(' ', '~').matchesAllOf(s)) {
            return "'" + s + "'";
        } else {
            StringBuilder builder = new StringBuilder();
            builder.append("U&'");
            PrimitiveIterator.OfInt iterator = s.codePoints().iterator();

            while (iterator.hasNext()) {
                int codePoint = iterator.nextInt();
                Preconditions.checkArgument(codePoint >= 0, "Invalid UTF-8 encoding in characters: %s", s);
                if (isAsciiPrintable(codePoint)) {
                    char ch = (char) codePoint;
                    if (ch == '\\') {
                        builder.append(ch);
                    }

                    builder.append(ch);
                } else if (codePoint <= 65535) {
                    builder.append('\\');
                    builder.append(String.format("%04X", codePoint));
                } else {
                    builder.append("\\+");
                    builder.append(String.format("%06X", codePoint));
                }
            }

            builder.append("'");
            return builder.toString();
        }
    }

    public static List<String> formatOrderBy(OrderBy orderBy) {
        return formatSortItems(orderBy.getSortItems());
    }

    private static List<String> formatSortItems(List<SortItem> sortItems) {
        return sortItems.stream().map(sortItemFormatterFunction()).collect(Collectors.toList());
    }

    private static boolean isAsciiPrintable(int codePoint) {
        return codePoint >= 32 && codePoint < 127;
    }

    private static String formatGroupingSet(List<Expression> groupingSet) {
        return String.format("(%s)", Joiner.on(", ").join(groupingSet.stream()
                .map(io.prestosql.sql.ExpressionFormatter::formatExpression).iterator()));
    }

    private static Function<SortItem, String> sortItemFormatterFunction() {
        return (input) -> {
            StringBuilder builder = new StringBuilder();
            builder.append(formatExpression(input.getSortKey()));
            switch (input.getOrdering()) {
                case ASCENDING:
                    builder.append(" ASC");
                    break;
                case DESCENDING:
                    builder.append(" DESC");
                    break;
                default:
                    throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
            }

            switch (input.getNullOrdering()) {
                case FIRST:
                    builder.append(" NULLS FIRST");
                    break;
                case LAST:
                    builder.append(" NULLS LAST");
                case UNDEFINED:
                    break;
                default:
                    throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
            }

            return builder.toString();
        };
    }

    public static class Formatter extends AstVisitor<String, Void> {
        public Formatter() {
        }

        protected String visitNode(Node node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitRow(Row node, Void context) {
            return Joiner.on(", ").join(node.getItems().stream()
                    .map((child) -> this.process(child, context)).collect(Collectors.toList()));
        }

        protected String visitExpression(Expression node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitAtTimeZone(AtTimeZone node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitCurrentUser(CurrentUser node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitCurrentPath(CurrentPath node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitFormat(Format node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitCurrentTime(CurrentTime node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitExtract(Extract node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitBooleanLiteral(BooleanLiteral node, Void context) {
            return String.valueOf(node.getValue());
        }

        protected String visitStringLiteral(StringLiteral node, Void context) {
            return AerospikeExpressionParser.formatStringLiteral(node.getValue());
        }

        protected String visitCharLiteral(CharLiteral node, Void context) {
            return "CHAR " + AerospikeExpressionParser.formatStringLiteral(node.getValue());
        }

        protected String visitBinaryLiteral(BinaryLiteral node, Void context) {
            return "X'" + node.toHexString() + "'";
        }

        protected String visitParameter(Parameter node, Void context) {
            return "?";
        }

        protected String visitAllRows(AllRows node, Void context) {
            return "ALL";
        }

        protected String visitArrayConstructor(ArrayConstructor node, Void context) {
            ImmutableList.Builder<String> valueStrings = ImmutableList.builder();

            for (Expression value : node.getValues()) {
                valueStrings.add(SqlFormatter.formatSql(value));
            }

            return "ARRAY[" + Joiner.on(",").join(valueStrings.build()) + "]";
        }

        protected String visitSubscriptExpression(SubscriptExpression node, Void context) {
            return SqlFormatter.formatSql(node.getBase()) + "[" + SqlFormatter.formatSql(node.getIndex()) + "]";
        }

        protected String visitLongLiteral(LongLiteral node, Void context) {
            return Long.toString(node.getValue());
        }

        protected String visitDoubleLiteral(DoubleLiteral node, Void context) {
            return (AerospikeExpressionParser.doubleFormatter.get()).format(node.getValue());
        }

        protected String visitDecimalLiteral(DecimalLiteral node, Void context) {
            return "DECIMAL '" + node.getValue() + "'";
        }

        protected String visitGenericLiteral(GenericLiteral node, Void context) {
            return node.getType() + " " + AerospikeExpressionParser.formatStringLiteral(node.getValue());
        }

        protected String visitTimeLiteral(TimeLiteral node, Void context) {
            return "TIME '" + node.getValue() + "'";
        }

        protected String visitTimestampLiteral(TimestampLiteral node, Void context) {
            return "TIMESTAMP '" + node.getValue() + "'";
        }

        protected String visitNullLiteral(NullLiteral node, Void context) {
            return "null";
        }

        protected String visitIntervalLiteral(IntervalLiteral node, Void context) {
            String sign = node.getSign() == IntervalLiteral.Sign.NEGATIVE ? "- " : "";
            StringBuilder builder = (new StringBuilder()).append("INTERVAL ").append(sign).append(" '")
                    .append(node.getValue()).append("' ").append(node.getStartField());
            if (node.getEndField().isPresent()) {
                builder.append(" TO ").append(node.getEndField().get());
            }

            return builder.toString();
        }

        protected String visitSubqueryExpression(SubqueryExpression node, Void context) {
            return "(" + SqlFormatter.formatSql(node.getQuery()) + ")";
        }

        protected String visitExists(ExistsPredicate node, Void context) {
            return "(EXISTS " + SqlFormatter.formatSql(node.getSubquery()) + ")";
        }

        protected String visitIdentifier(Identifier node, Void context) {
            return !node.isDelimited() ? node.getValue() : '"' + node.getValue().replace("\"", "\"\"") + '"';
        }

        protected String visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, Void context) {
            return io.prestosql.sql.ExpressionFormatter.formatExpression(node.getName());
        }

        protected String visitSymbolReference(SymbolReference node, Void context) {
            return AerospikeExpressionParser.formatIdentifier(node.getName());
        }

        protected String visitDereferenceExpression(DereferenceExpression node, Void context) {
            String baseString = this.process(node.getBase(), context);
            return baseString + "." + this.process(node.getField());
        }

        public String visitFieldReference(FieldReference node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitFunctionCall(FunctionCall node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitLambdaExpression(LambdaExpression node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitBindExpression(BindExpression node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
            return this.formatBinaryExpression(node.getOperator().toString(), node.getLeft(), node.getRight());
        }

        protected String visitNotExpression(NotExpression node, Void context) {
            return "(NOT " + this.process(node.getValue(), context) + ")";
        }

        protected String visitComparisonExpression(ComparisonExpression node, Void context) {
            return this.formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        protected String visitIsNullPredicate(IsNullPredicate node, Void context) {
            return "(" + this.process(node.getValue(), context) + " IS NULL)";
        }

        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
            return "(" + this.process(node.getValue(), context) + " IS NOT NULL)";
        }

        protected String visitNullIfExpression(NullIfExpression node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitIfExpression(IfExpression node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitTryExpression(TryExpression node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitCoalesceExpression(CoalesceExpression node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitLikePredicate(LikePredicate node, Void context) {
            StringBuilder builder = new StringBuilder();
            builder.append('(').append(this.process(node.getValue(), context)).append(" LIKE ")
                    .append(this.process(node.getPattern(), context));
            node.getEscape().ifPresent((escape) -> {
                builder.append(" ESCAPE ").append(this.process(escape, context));
            });
            builder.append(')');
            return builder.toString();
        }

        protected String visitAllColumns(AllColumns node, Void context) {
            StringBuilder builder = new StringBuilder();
            if (node.getTarget().isPresent()) {
                builder.append(this.process(node.getTarget().get(), context));
                builder.append(".*");
            } else {
                builder.append("*");
            }

            if (!node.getAliases().isEmpty()) {
                builder.append(" AS (");
                Joiner.on(", ").appendTo(builder, node.getAliases().stream().map((alias) -> this.process(alias, context))
                        .collect(Collectors.toList()));
                builder.append(")");
            }

            return builder.toString();
        }

        public String visitCast(Cast node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitWhenClause(WhenClause node, Void context) {
            return "WHEN " + this.process(node.getOperand(), context) + " THEN " + this.process(node.getResult(), context);
        }

        protected String visitBetweenPredicate(BetweenPredicate node, Void context) {
            return "(" + this.process(node.getValue(), context) + " BETWEEN " + this.process(node.getMin(), context)
                    + " AND " + this.process(node.getMax(), context) + ")";
        }

        protected String visitInPredicate(InPredicate node, Void context) {
            return "(" + this.process(node.getValue(), context) + " IN " + this.process(node.getValueList(), context) + ")";
        }

        protected String visitInListExpression(InListExpression node, Void context) {
            return "(" + this.joinExpressions(node.getValues()) + ")";
        }

        private String visitFilter(Expression node, Void context) {
            return "(WHERE " + this.process(node, context) + ')';
        }

        public String visitWindow(Window node, Void context) {
            throw new UnsupportedOperationException();
        }

        public String visitWindowFrame(WindowFrame node, Void context) {
            throw new UnsupportedOperationException();
        }

        public String visitFrameBound(FrameBound node, Void context) {
            throw new UnsupportedOperationException();
        }

        protected String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Void context) {
            return "(" + this.process(node.getValue(), context) + ' ' + node.getOperator().getValue() + ' '
                    + node.getQuantifier().toString() + ' ' + this.process(node.getSubquery(), context) + ")";
        }

        protected String visitGroupingOperation(GroupingOperation node, Void context) {
            return "GROUPING (" + this.joinExpressions(node.getGroupingColumns()) + ")";
        }

        protected String visitRowDataType(RowDataType node, Void context) {
            return node.getFields().stream().map(this::process)
                    .collect(Collectors.joining(", ", "ROW(", ")"));
        }

        protected String visitRowField(RowDataType.Field node, Void context) {
            StringBuilder result = new StringBuilder();
            if (node.getName().isPresent()) {
                result.append(this.process(node.getName().get(), context));
                result.append(" ");
            }

            result.append(this.process(node.getType(), context));
            return result.toString();
        }

        protected String visitGenericDataType(GenericDataType node, Void context) {
            StringBuilder result = new StringBuilder();
            result.append(node.getName());
            if (!node.getArguments().isEmpty()) {
                result.append(node.getArguments().stream().map(this::process)
                        .collect(Collectors.joining(", ", "(", ")")));
            }

            return result.toString();
        }

        protected String visitTypeParameter(TypeParameter node, Void context) {
            return this.process(node.getValue(), context);
        }

        protected String visitNumericTypeParameter(NumericParameter node, Void context) {
            return node.getValue();
        }

        protected String visitIntervalDataType(IntervalDayTimeDataType node, Void context) {
            StringBuilder builder = new StringBuilder();
            builder.append("INTERVAL ");
            builder.append(node.getFrom());
            if (node.getFrom() != node.getTo()) {
                builder.append(" TO ").append(node.getTo());
            }

            return builder.toString();
        }

        protected String visitDateTimeType(DateTimeDataType node, Void context) {
            StringBuilder builder = new StringBuilder();
            builder.append(node.getType().toString().toLowerCase(Locale.ENGLISH));
            if (node.getPrecision().isPresent()) {
                builder.append("(").append(node.getPrecision().get()).append(")");
            }

            if (node.isWithTimeZone()) {
                builder.append(" with time zone");
            }

            return builder.toString();
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right) {
            return left + operator + right;
        }

        private String joinExpressions(List<Expression> expressions) {
            throw new UnsupportedOperationException();
        }
    }
}
