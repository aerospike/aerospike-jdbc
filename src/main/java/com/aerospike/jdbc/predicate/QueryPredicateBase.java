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
        if (value == null) {
            return Exp.Type.NIL;
        } else if (value instanceof String) {
            return Exp.Type.STRING;
        } else if (value instanceof Long || value instanceof Integer
                || value instanceof Short || value instanceof Byte) {
            return Exp.Type.INT;
        } else if (value instanceof Double || value instanceof Float) {
            return Exp.Type.FLOAT;
        } else if (value instanceof Boolean) {
            return Exp.Type.BOOL;
        } else if (value instanceof byte[]) {
            return Exp.Type.BLOB;
        } else {
            return Exp.Type.STRING;
        }
    }

    protected Exp getValueExp(Object value) {
        if (value == null) {
            return Exp.nil();
        } else if (value instanceof String) {
            return Exp.val((String) value);
        } else if (value instanceof Long) {
            return Exp.val((long) value);
        } else if (value instanceof Integer) {
            return Exp.val((int) value);
        } else if (value instanceof Short) {
            return Exp.val((short) value);
        } else if (value instanceof Byte) {
            return Exp.val((byte) value);
        } else if (value instanceof Double) {
            return Exp.val((double) value);
        } else if (value instanceof Float) {
            return Exp.val((float) value);
        } else if (value instanceof Boolean) {
            return Exp.val((boolean) value);
        } else if (value instanceof byte[]) {
            return Exp.val((byte[]) value);
        } else {
            return Exp.val(value.toString());
        }
    }

    protected Exp buildLeftExp() {
        return binName.equals(Constants.PRIMARY_KEY_COLUMN_NAME)
                ? Exp.key(valueType)
                : Exp.bin(binName, valueType);
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
