package com.aerospike.jdbc.model;

import com.aerospike.jdbc.predicate.*;
import com.google.common.base.CharMatcher;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlVisitor;

import java.math.BigDecimal;
import java.util.stream.Collectors;

import static com.aerospike.jdbc.util.Constants.unsupportedQueryType;
import static java.util.Objects.requireNonNull;

public class AerospikeSqlVisitor implements SqlVisitor<AerospikeQuery> {

    private final AerospikeQuery query;

    public AerospikeSqlVisitor() {
        query = new AerospikeQuery();
    }

    @Override
    public AerospikeQuery visit(SqlLiteral sqlLiteral) {
        return query;
    }

    @Override
    public AerospikeQuery visit(SqlCall sqlCall) {
        try {
            if (sqlCall instanceof SqlSelect) {
                SqlSelect sql = (SqlSelect) sqlCall;
                query.setQueryType(QueryType.SELECT);
                query.setTable(requireNonNull(sql.getFrom()).toString());
                query.setColumns(sql.getSelectList().stream().map(SqlNode::toString).collect(Collectors.toList()));
                if (sql.hasWhere()) {
                    query.setPredicate(parseWhere((SqlBasicCall) requireNonNull(sql.getWhere())));
                }
            } else if (sqlCall instanceof SqlUpdate) {
                SqlUpdate sql = (SqlUpdate) sqlCall;
                query.setQueryType(QueryType.UPDATE);
                query.setTable(requireNonNull(sql.getTargetTable()).toString());
                if (sql.getCondition() != null) {
                    query.setPredicate(parseWhere((SqlBasicCall) sql.getCondition()));
                }
                query.setColumns(sql.getTargetColumnList().stream().map(SqlNode::toString).collect(Collectors.toList()));
                query.setValues(sql.getSourceExpressionList().stream().map(this::parseValue).collect(Collectors.toList()));
            } else if (sqlCall instanceof SqlInsert) {
                SqlInsert sql = (SqlInsert) sqlCall;
                query.setQueryType(QueryType.INSERT);
                query.setTable(requireNonNull(sql.getTargetTable()).toString());
                query.setColumns(requireNonNull(sql.getTargetColumnList()).stream()
                        .map(SqlNode::toString).collect(Collectors.toList()));
                query.setValues(
                        ((SqlBasicCall) sql.getSource()).getOperandList()
                                .stream().map(SqlBasicCall.class::cast)
                                .map(c -> c.getOperandList()
                                        .stream().map(this::parseValue).collect(Collectors.toList()))
                                .collect(Collectors.toList())
                );
            } else if (sqlCall instanceof SqlDelete) {
                SqlDelete sql = (SqlDelete) sqlCall;
                query.setQueryType(QueryType.DELETE);
                query.setTable(requireNonNull(sql.getTargetTable()).toString());
                if (sql.getCondition() != null) {
                    query.setPredicate(parseWhere((SqlBasicCall) sql.getCondition()));
                }
            } else if (sqlCall instanceof SqlDropTable) {
                SqlDropTable sql = (SqlDropTable) sqlCall;
                query.setTable(requireNonNull(sql.name).toString());
                query.setQueryType(QueryType.DROP_TABLE);
            } else if (sqlCall instanceof SqlDropSchema) {
                SqlDropSchema sql = (SqlDropSchema) sqlCall;
                query.setSchema(requireNonNull(sql.name).toString());
                query.setQueryType(QueryType.DROP_SCHEMA);
            } else if (sqlCall instanceof SqlOrderBy) {
                SqlOrderBy sql = (SqlOrderBy) sqlCall;
                if (!sql.orderList.isEmpty()) throw new UnsupportedOperationException(unsupportedQueryType);
                if (sql.fetch != null) {
                    query.setLimit(requireNonNull((BigDecimal) ((SqlNumericLiteral) sql.fetch).getValue()).intValue());
                }
                if (sql.offset != null) {
                    query.setOffset(requireNonNull((BigDecimal) ((SqlNumericLiteral) sql.offset).getValue()).intValue());
                }
                visit((SqlCall) sql.query);
            } else {
                throw new UnsupportedOperationException(unsupportedQueryType);
            }
        } catch (Exception e) {
            throw new UnsupportedOperationException(unsupportedQueryType);
        }
        return query;
    }

    private QueryPredicate parseWhere(SqlBasicCall where) {
        if (where.getOperator() instanceof SqlBinaryOperator) {
            Operator operator = Operator.parsed(where.getOperator());
            if (Operator.isBoolean(operator)) {
                return new QueryPredicateBoolean(
                        parseWhere((SqlBasicCall) where.getOperandList().get(0)),
                        operator,
                        parseWhere((SqlBasicCall) where.getOperandList().get(1))
                );
            } else if (Operator.isVarArgs(operator)) {
                return new QueryPredicateList(
                        where.getOperandList().get(0).toString(),
                        operator,
                        ((SqlNodeList) where.getOperandList().get(1)).stream().map(this::parseValue).distinct().toArray()
                );
            } else {
                return new QueryPredicateBinary(
                        where.getOperandList().get(0).toString(),
                        operator,
                        parseValue(where.getOperandList().get(1))
                );
            }
        } else if (where.getOperator() instanceof SqlPrefixOperator) {
            Operator operator = Operator.parsed(where.getOperator());
            return new QueryPredicatePrefix(
                    operator,
                    parseWhere((SqlBasicCall) where.getOperandList().get(0))
            );
        } else if (where.getOperator() instanceof SqlPostfixOperator) {
            if (where.getOperator().kind == SqlKind.IS_NULL) {
                String binName = where.getOperandList().get(0).toString();
                return new QueryPredicateIsNull(binName);
            } else if (where.getOperator().kind == SqlKind.IS_NOT_NULL) {
                String binName = where.getOperandList().get(0).toString();
                return new QueryPredicateIsNotNull(binName);
            }
        } else if (where.getOperator() instanceof SqlLikeOperator) {
            String binName = where.getOperandList().get(0).toString();
            String expression = unwrapString(where.getOperandList().get(1).toString());
            return new QueryPredicateLike(binName, expression);
        } else if (where.getOperator() instanceof SqlBetweenOperator) {
            return new QueryPredicateRange(
                    where.getOperandList().get(0).toString(),
                    parseValue(where.getOperandList().get(1)),
                    parseValue(where.getOperandList().get(2))
            );
        }
        throw new UnsupportedOperationException(unsupportedQueryType);
    }

    private Object parseValue(SqlNode sqlNode) {
        if (sqlNode instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral) sqlNode;
            if (literal instanceof SqlNumericLiteral) {
                return getNumeric((BigDecimal) literal.getValue());
            } else if (literal instanceof SqlCharStringLiteral) {
                return unwrapString(requireNonNull(literal.getValue()).toString());
            } else {
                if (literal.getTypeName() == SqlTypeName.NULL || literal.getTypeName() == SqlTypeName.BOOLEAN) {
                    return literal.getValue();
                }
            }
        } else if (sqlNode instanceof SqlIdentifier) {
            return unwrapString(sqlNode.toString());
        }
        throw new UnsupportedOperationException(unsupportedQueryType);
    }

    private Object getNumeric(BigDecimal bd) {
        requireNonNull(bd);
        if (bd.signum() == 0 || bd.scale() <= 0 || bd.stripTrailingZeros().scale() <= 0) {
            return bd.longValue();
        }
        return bd.doubleValue();
    }

    private String unwrapString(String str) {
        str = CharMatcher.is('\"').trimFrom(str);
        return CharMatcher.is('\'').trimFrom(str);
    }

    @Override
    public AerospikeQuery visit(SqlNodeList sqlNodeList) {
        return query;
    }

    @Override
    public AerospikeQuery visit(SqlIdentifier sqlIdentifier) {
        return query;
    }

    @Override
    public AerospikeQuery visit(SqlDataTypeSpec sqlDataTypeSpec) {
        return query;
    }

    @Override
    public AerospikeQuery visit(SqlDynamicParam sqlDynamicParam) {
        return query;
    }

    @Override
    public AerospikeQuery visit(SqlIntervalQualifier sqlIntervalQualifier) {
        return query;
    }
}
