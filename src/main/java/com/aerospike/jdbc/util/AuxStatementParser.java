package com.aerospike.jdbc.util;

import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.QueryType;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aerospike.jdbc.util.Constants.UNSUPPORTED_QUERY_TYPE_MESSAGE;

public final class AuxStatementParser {

    private static final Pattern truncateTablePattern;
    private static final Pattern createIndexPattern;
    private static final Pattern dropIndexPattern;

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
    }

    private AuxStatementParser() {
    }

    /**
     * An auxiliary method to parse queries which are not supported by the main parser.
     *
     * @param sql the original SQL query string.
     * @return an {@link com.aerospike.jdbc.model.AerospikeQuery}
     * @throws SQLException if no match.
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
