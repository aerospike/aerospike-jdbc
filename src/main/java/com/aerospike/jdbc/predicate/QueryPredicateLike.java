package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RegexFlag;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class QueryPredicateLike extends QueryPredicateBase {

    private final String expression;

    public QueryPredicateLike(String binName, String expression) {
        super(binName, Exp.Type.STRING);
        this.expression = expression.replace("%", ".*");
    }

    @Override
    public Exp toFilterExpression(boolean withPrimaryKey) {
        return Exp.regexCompare(
                expression,
                RegexFlag.ICASE | RegexFlag.NEWLINE,
                buildLeftExp()
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
