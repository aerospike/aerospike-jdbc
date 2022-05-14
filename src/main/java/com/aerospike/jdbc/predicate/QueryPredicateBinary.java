package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;

import java.util.Optional;

import static com.aerospike.jdbc.util.Constants.defaultKeyName;

public class QueryPredicateBinary extends QueryPredicateBase {

    private final Operator operator;
    private final Object value;

    public QueryPredicateBinary(String binName, Operator operator, Object value) {
        super(binName, getValueType(value));
        this.operator = operator;
        this.value = value;
    }

    private static Exp.Type getValueType(Object value) {
        if (value instanceof String) {
            return Exp.Type.STRING;
        } else if (value instanceof Integer) {
            return Exp.Type.INT;
        } else if (value instanceof Double) {
            return Exp.Type.FLOAT;
        } else if (value instanceof Boolean) {
            return Exp.Type.BOOL;
        } else {
            return Exp.Type.STRING;
        }
    }

    private Exp getValueExp() {
        if (value instanceof String) {
            return Exp.val((String) value);
        } else if (value instanceof Integer) {
            return Exp.val((int) value);
        } else if (value instanceof Double) {
            return Exp.val((double) value);
        } else if (value instanceof Boolean) {
            return Exp.val((boolean) value);
        } else {
            return Exp.stringBin(value.toString());
        }
    }

    @Override
    public Exp toFilterExpression() {
        return operator.exp(buildLeftExp(), getValueExp());
    }

    @Override
    public Optional<Filter> toFilter(String binName) {
        if (binName.equals(this.binName) && operator == OperatorBinary.EQ) {
            if (valueType == Exp.Type.INT) {
                return Optional.of(Filter.equal(binName, (int) value));
            }
            return Optional.of(Filter.equal(binName, (String) value));
        }
        return Optional.empty();
    }

    @Override
    public Object getPrimaryKey() {
        if (binName.equals(defaultKeyName)) {
            return value;
        }
        return null;
    }
}
