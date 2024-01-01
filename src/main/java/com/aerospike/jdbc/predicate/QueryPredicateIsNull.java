package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;

public class QueryPredicateIsNull implements QueryPredicate {

    private final String binName;

    public QueryPredicateIsNull(String binName) {
        this.binName = binName;
    }

    @Override
    public Exp toFilterExpression(boolean withPrimaryKey) {
        return Exp.not(
                binName.equals(PRIMARY_KEY_COLUMN_NAME)
                        ? Exp.keyExists()
                        : Exp.binExists(binName)
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
    public List<String> getBinNames() {
        return Collections.emptyList();
    }
}
