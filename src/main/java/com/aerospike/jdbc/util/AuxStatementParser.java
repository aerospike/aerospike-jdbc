package com.aerospike.jdbc.util;

import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.QueryType;
import com.aerospike.jdbc.query.AerospikeQueryParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aerospike.jdbc.AerospikeStatement.SQL_PARSER;
import static com.aerospike.jdbc.AerospikeStatement.parsingOptions;

public final class AuxStatementParser {

    private static final Pattern updateSetWherePattern;
    private static final Pattern updateSetPattern;

    private static final Pattern truncateTablePattern;

    static {
        String p1 = "update (.*) set (.*) where (.*)";
        updateSetWherePattern = Pattern.compile(p1, Pattern.CASE_INSENSITIVE);

        String p2 = "update (.*) set (.*)";
        updateSetPattern = Pattern.compile(p2, Pattern.CASE_INSENSITIVE);

        String p3 = "truncate table (.*)";
        truncateTablePattern = Pattern.compile(p3, Pattern.CASE_INSENSITIVE);
    }

    private AuxStatementParser() {
    }

    /**
     * A hack method to parse queries which are currently not supported by the Presto parser.
     *
     * @param sql the original SQL query string.
     * @return an Optional with a present {@link com.aerospike.jdbc.model.AerospikeQuery}
     * for the valid statement, otherwise an empty Optional.
     */
    public static Optional<AerospikeQuery> hack(String sql) {
        Matcher m = updateSetWherePattern.matcher(sql);
        if (m.find()) {
            String queryString = "select * from " + m.group(1) + " where " + m.group(3);
            return Optional.of(buildQuery(queryString, m.group(2)));
        }

        m = updateSetPattern.matcher(sql);
        if (m.find()) {
            String queryString = "select * from " + m.group(1);
            return Optional.of(buildQuery(queryString, m.group(2)));
        }

        m = truncateTablePattern.matcher(sql);
        if (m.find()) {
            AerospikeQuery query = new AerospikeQuery();
            query.setType(QueryType.DROP_TABLE);
            query.setTable(m.group(1));
            return Optional.of(query);
        }

        return Optional.empty();
    }

    private static AerospikeQuery buildQuery(String queryString, String setString) {
        final List<String> columns = new ArrayList<>();
        final List<String> values = new ArrayList<>();
        Arrays.stream(setString.split(",")).map(String::trim).forEach(s -> {
            String[] arr = s.split("=");
            columns.add(arr[0]);
            values.add(arr[1]);
        });
        io.trino.sql.tree.Statement statement = SQL_PARSER.createStatement(queryString, parsingOptions);
        AerospikeQuery query = AerospikeQueryParser.parseSql(statement);

        query.setColumns(columns);
        query.setValues(values);
        query.setType(QueryType.UPDATE);

        return query;
    }
}
