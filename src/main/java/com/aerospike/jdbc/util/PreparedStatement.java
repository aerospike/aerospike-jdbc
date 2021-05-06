package com.aerospike.jdbc.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;

public class PreparedStatement {

    public static final char PS_PLACEHOLDER_PREFIX = '_';

    private PreparedStatement() {
    }

    public static Entry<String, Integer> parseParameters(String sql, int offset) {
        StringBuilder fixedIndexSqlBuf = new StringBuilder();
        int count = 0;
        boolean intoConstant = false;
        for (char c : sql.toCharArray()) {
            if (c == '\'') {
                intoConstant = !intoConstant;
            }
            if (!intoConstant && c == '?') {
                count++;
                fixedIndexSqlBuf.append(PS_PLACEHOLDER_PREFIX).append(count + offset);
            } else {
                fixedIndexSqlBuf.append(c);
            }
        }
        return Collections.singletonMap(fixedIndexSqlBuf.toString(), count).entrySet().iterator().next();
    }

    public static Iterable<String> splitQueries(String sql) {
        Collection<String> queries = new ArrayList<>();
        StringBuilder currentQuery = new StringBuilder();
        boolean intoConstant = false;

        for (char c : sql.toCharArray()) {
            if (c == '\'') {
                intoConstant = !intoConstant;
            }
            if (!intoConstant && c == ';') {
                appendNotEmpty(queries, currentQuery.toString());
                currentQuery.setLength(0);
            } else {
                currentQuery.append(c);
            }
        }

        if (currentQuery.length() > 0 && currentQuery.toString().trim().length() > 0) {
            appendNotEmpty(queries, currentQuery.toString());
        }

        return queries;
    }

    private static void appendNotEmpty(Collection<String> queries, String query) {
        if (query.trim().length() > 0) {
            queries.add(query);
        }
    }
}
