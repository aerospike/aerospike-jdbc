package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class QueryPredicatePrefix implements QueryPredicate {

    private final Operator operator;
    private final QueryPredicate right;

    public QueryPredicatePrefix(
            Operator operator,
            QueryPredicate right
    ) {
        this.operator = operator;
        this.right = right;
    }

    @Override
    public Exp toFilterExpression(boolean withPrimaryKey) {
        Exp rightExp = right.toFilterExpression(withPrimaryKey);
        if (rightExp == null) {
            return null;
        }
        return operator.exp(rightExp);
    }

    @Override
    public Optional<Filter> toFilter(String binName) {
        return Optional.empty();
    }

    @Override
    public boolean isIndexable() {
        return false;
    }

    @Override
    public List<String> getBinNames() {
        return Collections.emptyList();
    }
}
