package com.aerospike.jdbc.util;

import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.QueryType;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aerospike.jdbc.util.Constants.UNSUPPORTED_QUERY_TYPE_MESSAGE;

public final class AuxStatementParser {

    private static final Pattern truncateTablePattern;
    private static final Pattern createIndexPattern;
    private static final Pattern dropIndexPattern;
    private static final Pattern explainPattern;

    static {
        truncateTablePattern = Pattern.compile(
                "truncate\\s+table\\s+\"?([^\\s^;\"]+)\"?[\\s;]*",
                Pattern.CASE_INSENSITIVE);
        createIndexPattern = Pattern.compile(
                "create\\s+index\\s+(\\S+)\\s+on\\s+\"?([^\\s^;\"]+)\"?\\s*\\((.+)\\)[\\s;]*",
                Pattern.CASE_INSENSITIVE);
        dropIndexPattern = Pattern.compile(
                "drop\\s+index\\s+(\\S+)\\s+on\\s*\"?([^\\s^;\"]+)\"?[\\s;]*",
                Pattern.CASE_INSENSITIVE);
        explainPattern = Pattern.compile(
                "explain\\s+(.+)",
                Pattern.CASE_INSENSITIVE);
    }

    private AuxStatementParser() {
    }

    /**
     * Parses an SQL query with parameters and returns an {@link AerospikeQuery} object.
     *
     * @param sql           The SQL query string to parse.
     * @param sqlParameters A list of parameters to be used in the SQL query.
     * @return An {@link AerospikeQuery} object representing the parsed query.
     * @throws SQLException If an error occurs during parsing.
     */
    public static AerospikeQuery parse(String sql, List<Object> sqlParameters) throws SQLException {
        try {
            // Attempt to parse the SQL query directly.
            return AerospikeQuery.parse(sql, sqlParameters);
        } catch (Exception e) {
            // Check if the query is an EXPLAIN statement.
            Matcher m = explainPattern.matcher(sql);
            if (m.find()) {
                try {
                    AerospikeQuery query = AerospikeQuery.parse(m.group(1), sqlParameters);
                    query.setQueryType(QueryType.EXPLAIN);
                    return query;
                } catch (Exception ex) {
                    throw new SQLException(ex);
                }
            }
            // Attempt to parse other non-standard query types.
            return parse(sql);
        }
    }

    /**
     * Parses SQL queries that are not supported by the standard parser.
     *
     * @param sql The original SQL query string.
     * @return An {@link AerospikeQuery} object representing the parsed query.
     * @throws SQLException If the query does not match any of the supported patterns.
     */
    public static AerospikeQuery parse(String sql) throws SQLException {
        Matcher m = truncateTablePattern.matcher(sql);
        if (m.find()) {
            AerospikeQuery query = new AerospikeQuery();
            query.setQueryType(QueryType.DROP_TABLE);
            query.setTable(m.group(1));
            return query;
        }

        m = createIndexPattern.matcher(sql);
        if (m.find()) {
            AerospikeQuery query = new AerospikeQuery();
            query.setQueryType(QueryType.CREATE_INDEX);
            query.setIndex(m.group(1));
            query.setTable(m.group(2));
            query.setColumns(Arrays.asList(m.group(3).trim().split(",")));
            return query;
        }

        m = dropIndexPattern.matcher(sql);
        if (m.find()) {
            AerospikeQuery query = new AerospikeQuery();
            query.setQueryType(QueryType.DROP_INDEX);
            query.setIndex(m.group(1));
            query.setTable(m.group(2));
            return query;
        }

        throw new SQLDataException(UNSUPPORTED_QUERY_TYPE_MESSAGE);
    }
}
