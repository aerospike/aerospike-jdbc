package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.jdbc.util.Constants;

import java.util.Collections;
import java.util.List;

public abstract class QueryPredicateBase implements QueryPredicate {

    protected final String binName;
    protected final Exp.Type valueType;

    public QueryPredicateBase(
            String binName,
            Exp.Type valueType) {
        this.binName = binName;
        this.valueType = valueType;
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
