package com.aerospike.jdbc.util;

import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.QueryType;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aerospike.jdbc.util.Constants.UNSUPPORTED_QUERY_TYPE_MESSAGE;

public final class AuxStatementParser {

    private static final Pattern truncateTablePattern;

    static {
        truncateTablePattern = Pattern.compile("truncate table (.*)", Pattern.CASE_INSENSITIVE);
    }

    private AuxStatementParser() {
    }

    /**
     * An auxiliary method to parse queries which are currently not supported by the parser.
     *
     * @param sql the original SQL query string.
     * @return an {@link com.aerospike.jdbc.model.AerospikeQuery}
     * @throws SQLException if no match.
     */
    public static AerospikeQuery hack(String sql) throws SQLException {
        Matcher m = truncateTablePattern.matcher(sql);
        if (m.find()) {
            AerospikeQuery query = new AerospikeQuery();
            query.setQueryType(QueryType.DROP_TABLE);
            query.setTable(m.group(1));
            return query;
        }

        throw new SQLException(UNSUPPORTED_QUERY_TYPE_MESSAGE);
    }
}
