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

public final class UpdateStatemenParser {

    private static final Pattern pattern;

    static {
        String str = "update (.*) set (.*) where (.*)";
        pattern = Pattern.compile(str, Pattern.CASE_INSENSITIVE);
    }

    private UpdateStatemenParser() {
    }

    /**
     * A hack method to parse UPDATE queries which are currently not supported by Presto.
     *
     * @param sql the original SQL query string.
     * @return an Optional with a present {@link com.aerospike.jdbc.model.AerospikeQuery}
     * for the valid UPDATE statement, otherwise an empty Optional.
     */
    public static Optional<AerospikeQuery> hack(String sql) {
        Matcher m = pattern.matcher(sql);
        if (m.find()) {
            String queryString = "select * from " + m.group(1) + " where " + m.group(3);
            final List<String> columns = new ArrayList<>();
            final List<String> values = new ArrayList<>();
            Arrays.stream(m.group(2).split(",")).map(String::trim).forEach(s -> {
                String[] arr = s.split("=");
                columns.add(arr[0]);
                values.add(arr[1]);
            });
            io.prestosql.sql.tree.Statement statement = SQL_PARSER.createStatement(queryString, parsingOptions);
            AerospikeQuery query = AerospikeQueryParser.parseSql(statement);

            query.setColumns(columns);
            query.setValues(values);
            query.setType(QueryType.UPDATE);

            return Optional.of(query);
        }
        return Optional.empty();
    }
}
