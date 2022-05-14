package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;

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
    public Exp toFilterExpression() {
        return operator.exp(right.toFilterExpression(), null);
    }

    @Override
    public Optional<Filter> toFilter(String binName) {
        return Optional.empty();
    }

    @Override
    public boolean isIndexable() {
        return true;
    }

    @Override
    public List<String> getBinNames() {
        return right.getBinNames();
    }
}
