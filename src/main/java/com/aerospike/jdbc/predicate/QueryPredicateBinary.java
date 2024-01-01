package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public class QueryPredicateBinary extends QueryPredicateBase {

    private final Operator operator;
    private final Object value;

    public QueryPredicateBinary(String binName, Operator operator, Object value) {
        super(binName, getValueType(value));
        this.operator = operator;
        this.value = value;
    }

    @Override
    public Exp toFilterExpression(boolean withPrimaryKey) {
        if (isPrimaryKeyPredicate() && !withPrimaryKey) {
            return null;
        }
        return operator.exp(buildLeftExp(), getValueExp(value));
    }

    @Override
    public Optional<Filter> toFilter(String binName) {
        if (binName.equals(this.binName) && operator == OperatorBinary.EQ) {
            if (valueType == Exp.Type.INT) {
                return Optional.of(Filter.equal(binName, (long) value));
            } else if (valueType == Exp.Type.STRING) {
                return Optional.of(Filter.equal(binName, (String) value));
            }
        }
        return Optional.empty();
    }

    @Override
    public Collection<Object> getPrimaryKeys() {
        if (isPrimaryKeyPredicate() && operator == OperatorBinary.EQ) {
            return Collections.singletonList(value);
        }
        return Collections.emptyList();
    }
}
