package com.aerospike.jdbc.query;

import com.aerospike.client.Value;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.RegexFlag;
import com.aerospike.jdbc.model.OpType;
import com.aerospike.jdbc.model.Pair;
import com.aerospike.jdbc.model.WhereExpression;

import java.util.Objects;
import java.util.function.BiFunction;

import static com.aerospike.jdbc.util.Constants.defaultKeyName;

public final class ExpressionBuilder {

    private ExpressionBuilder() {
    }

    public static Value fetchPrimaryKey(WhereExpression whereExpression) {
        if (Objects.nonNull(whereExpression) && !whereExpression.isWrapper()) {
            if (whereExpression.getColumn().equals(defaultKeyName)) {
                Object value = whereExpression.getValue();
                if (isStringValue(value.toString())) {
                    return Value.get(stripQuotes(value.toString()));
                }
                return Value.get(value);
            }
        }
        return null;
    }

    public static Exp buildExp(WhereExpression whereExpression) {
        if (Objects.isNull(whereExpression)) return null;
        if (whereExpression.isWrapper()) {
            OpType opType = whereExpression.getOpType();
            switch (opType) {
                case OR:
                    return Exp.or(
                            whereExpression.getInner().stream().map(ExpressionBuilder::buildExp).toArray(Exp[]::new)
                    );
                case AND:
                    return Exp.and(
                            whereExpression.getInner().stream().map(ExpressionBuilder::buildExp).toArray(Exp[]::new)
                    );
                case NOT:
                    return Exp.not(
                            whereExpression.getInner().stream().map(ExpressionBuilder::buildExp).toArray(Exp[]::new)[0]
                    );
                default:
                    throw new IllegalArgumentException("Unexpected OpType: " + whereExpression.getOpType());
            }
        }
        return buildComparisonExpession(whereExpression);
    }

    private static Exp buildComparisonExpession(WhereExpression whereExpression) {
        OpType opType = whereExpression.getOpType();
        switch (opType) {
            case LIKE:
                return Exp.regexCompare(
                        whereExpression.getValue().toString(),
                        RegexFlag.ICASE | RegexFlag.NEWLINE,
                        Exp.stringBin(whereExpression.getColumn())
                );
            case EQUALS:
                return getComparableExpression(whereExpression, Exp::eq);
            case LESS:
                return getComparableExpression(whereExpression, Exp::lt);
            case LESS_EQUALS:
                return getComparableExpression(whereExpression, Exp::le);
            case GREATER:
                return getComparableExpression(whereExpression, Exp::gt);
            case GREATER_EQUALS:
                return getComparableExpression(whereExpression, Exp::ge);
            case NOT_NULL:
                return Exp.binExists(whereExpression.getColumn());
            case NULL:
                return Exp.eq(Exp.binType(whereExpression.getColumn()), Exp.val(0));
            default:
                throw new IllegalArgumentException("Unexpected OpType: " + whereExpression.getOpType());
        }
    }

    private static Exp getComparableExpression(WhereExpression whereExpression,
                                               BiFunction<Exp, Exp, Exp> expFunc) {
        Pair<Exp, Exp> exps = parseComparable(whereExpression);
        return expFunc.apply(
                exps.getLeft(),
                exps.getRight()
        );
    }

    private static Pair<Exp, Exp> parseComparable(WhereExpression whereExpression) {
        if (isStringValue(whereExpression.getValue().toString())) {
            return new Pair<>(
                    Exp.stringBin(whereExpression.getColumn()),
                    Exp.val(stripQuotes(whereExpression.getValue().toString()))
            );
        } else {
            return new Pair<>(
                    Exp.intBin(whereExpression.getColumn()),
                    Exp.val(Long.parseLong(whereExpression.getValue().toString()))
            );
        }
    }

    private static boolean isStringValue(String value) {
        return value.startsWith("\"") && value.endsWith("\"");
    }

    private static String stripQuotes(String value) {
        return value.substring(1, value.length() - 1);
    }
}
