package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public interface QueryPredicate {

    Exp toFilterExpression(boolean withPrimaryKey);

    Optional<Filter> toFilter(String binName);

    boolean isIndexable();

    List<String> getBinNames();

    default Collection<Object> getPrimaryKeys() {
        return Collections.emptyList();
    }
}
