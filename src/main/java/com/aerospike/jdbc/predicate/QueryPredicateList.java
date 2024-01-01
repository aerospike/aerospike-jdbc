package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public class QueryPredicateList extends QueryPredicateBase {

    private final Operator operator;
    private final Object[] values;

    public QueryPredicateList(String binName, Operator operator, Object[] values) {
        super(binName, getValueType(values[0]));
        this.operator = operator;
        this.values = values;
    }

    @Override
    public Exp toFilterExpression(boolean withPrimaryKey) {
        if (isPrimaryKeyPredicate() && !withPrimaryKey) {
            return null;
        }
        return operator.exp(
                Arrays.stream(values)
                        .map(v -> Exp.eq(buildLeftExp(), getValueExp(v)))
                        .toArray(Exp[]::new)
        );
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
    public Collection<Object> getPrimaryKeys() {
        if (isPrimaryKeyPredicate()) {
            return Arrays.asList(values);
        }
        return Collections.emptyList();
    }
}
