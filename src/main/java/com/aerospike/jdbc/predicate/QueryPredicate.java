package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;

import java.util.List;
import java.util.Optional;

public interface QueryPredicate {

    Exp toFilterExpression();

    Optional<Filter> toFilter(String binName);

    boolean isIndexable();

    List<String> getBinNames();

    default Object getPrimaryKey() {
        return null;
    }
}
