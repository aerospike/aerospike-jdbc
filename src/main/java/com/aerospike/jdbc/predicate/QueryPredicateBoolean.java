package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryPredicateBoolean implements QueryPredicate {

    private final QueryPredicate left;
    private final Operator operator;
    private final QueryPredicate right;

    public QueryPredicateBoolean(
            QueryPredicate left,
            Operator operator,
            QueryPredicate right
    ) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static <T> Optional<T> or(Optional<T> optional, Optional<T> fallback) {
        return optional.isPresent() ? optional : fallback;
    }

    @Override
    public Exp toFilterExpression(boolean withPrimaryKey) {
        Exp leftExp = left.toFilterExpression(withPrimaryKey);
        Exp rightExp = right.toFilterExpression(withPrimaryKey);
        if (leftExp == null || rightExp == null) {
            return leftExp == null ? rightExp : leftExp;
        }
        return operator.exp(leftExp, rightExp);
    }

    @Override
    public Optional<Filter> toFilter(String binName) {
        if (isIndexable()) {
            // TODO: support range filters
            return or(
                    left.toFilter(binName),
                    right.toFilter(binName)
            );
        }
        return Optional.empty();
    }

    @Override
    public boolean isIndexable() {
        return operator == OperatorBinary.AND && (left.isIndexable() || right.isIndexable());
    }

    @Override
    public List<String> getBinNames() {
        if (isIndexable()) {
            return Stream.concat(left.getBinNames().stream(), right.getBinNames().stream())
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> getPrimaryKeys() {
        if (operator == OperatorBinary.AND) {
            return Stream.concat(left.getPrimaryKeys().stream(), right.getPrimaryKeys().stream())
                    .collect(Collectors.toSet());
        }
        return Collections.emptyList();
    }
}
