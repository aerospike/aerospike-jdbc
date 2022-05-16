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
    public Exp toFilterExpression() {
        return Exp.and(
                Exp.ge(buildLeftExp(), getValueExp(lowValue)),
                Exp.lt(buildLeftExp(), getValueExp(highValue))
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
