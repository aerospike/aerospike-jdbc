package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.jdbc.util.Constants;

import java.util.Collections;
import java.util.List;

public abstract class QueryPredicateBase implements QueryPredicate {

    protected final String binName;
    protected final Exp.Type valueType;

    protected QueryPredicateBase(
            String binName,
            Exp.Type valueType
    ) {
        this.binName = binName;
        this.valueType = valueType;
    }

    protected static Exp.Type getValueType(Object value) {
        if (value instanceof String) {
            return Exp.Type.STRING;
        } else if (value instanceof Long) {
            return Exp.Type.INT;
        } else if (value instanceof Double) {
            return Exp.Type.FLOAT;
        } else if (value instanceof Boolean) {
            return Exp.Type.BOOL;
        } else {
            return Exp.Type.STRING;
        }
    }

    protected Exp getValueExp(Object value) {
        if (value instanceof String) {
            return Exp.val((String) value);
        } else if (value instanceof Long) {
            return Exp.val((long) value);
        } else if (value instanceof Double) {
            return Exp.val((double) value);
        } else if (value instanceof Boolean) {
            return Exp.val((boolean) value);
        } else {
            return Exp.val(value.toString());
        }
    }

    protected Exp buildLeftExp() {
        return binName.equals(Constants.defaultKeyName) ? Exp.key(valueType) : Exp.bin(binName, valueType);
    }

    @Override
    public boolean isIndexable() {
        return true;
    }

    @Override
    public List<String> getBinNames() {
        if (isIndexable()) {
            return Collections.singletonList(binName);
        }
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + binName + ")";
    }
}
