package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.google.common.base.Preconditions;

import java.util.Optional;

public class QueryPredicateRange extends QueryPredicateBase {

    private final Object lowValue;
    private final Object highValue;

    public QueryPredicateRange(
            String binName,
            Object lowValue,
            Object highValue
    ) {
        super(binName, getValueType(lowValue));
        Preconditions.checkState(getValueType(lowValue) == getValueType(highValue));
        this.lowValue = lowValue;
        this.highValue = highValue;
    }

    @Override
    public Exp toFilterExpression(boolean withPrimaryKey) {
        // ANSI SQL defines the BETWEEN operator to be inclusive,
        // so both boundary values are included in the range.
        return Exp.and(
                Exp.ge(buildLeftExp(), getValueExp(lowValue)),
                Exp.le(buildLeftExp(), getValueExp(highValue))
        );
    }

    @Override
    public Optional<Filter> toFilter(String binName) {
        if (binName.equals(this.binName) && valueType == Exp.Type.INT) {
            return Optional.of(Filter.range(binName, (long) lowValue, (long) highValue));
        }
        return Optional.empty();
    }
}
