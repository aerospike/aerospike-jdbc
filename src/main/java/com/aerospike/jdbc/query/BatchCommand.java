package com.aerospike.jdbc.query;

import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.QueryType;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;

/**
 * A single command of a JDBC batch.
 */
public final class BatchCommand {

    private static final Set<QueryType> SUPPORTED_QUERY_TYPES = EnumSet.of(
            QueryType.INSERT,
            QueryType.UPDATE,
            QueryType.DELETE,
            QueryType.DROP_TABLE,
            QueryType.CREATE_INDEX,
            QueryType.DROP_INDEX
    );

    private final AerospikeQuery query;
    private final List<List<Object>> rows;

    private BatchCommand(AerospikeQuery query, List<List<Object>> rows) {
        this.query = query;
        this.rows = rows;
    }

    /**
     * Creates a command from a fully parsed query.
     */
    @SuppressWarnings("unchecked")
    public static BatchCommand of(AerospikeQuery query) {
        List<List<Object>> rows = null;
        if (query.getQueryType() == QueryType.INSERT) {
            rows = new ArrayList<>();
            for (Object row : query.getValues()) {
                rows.add(new ArrayList<>((List<Object>) row));
            }
        }
        return new BatchCommand(query, rows);
    }

    /**
     * Creates a single-row INSERT command that shares the parsed {@code template}.
     */
    public static BatchCommand insertRow(AerospikeQuery template, List<Object> row) {
        return new BatchCommand(template, Collections.singletonList(new ArrayList<>(row)));
    }

    public static void validate(AerospikeQuery query) throws SQLException {
        if (!SUPPORTED_QUERY_TYPES.contains(query.getQueryType())) {
            throw new SQLException(format("%s statements cannot be executed in a batch",
                    query.getQueryType()));
        }
    }

    public AerospikeQuery getQuery() {
        return query;
    }

    public QueryType getQueryType() {
        return query.getQueryType();
    }

    /**
     * INSERT rows; {@code null} for other statement types.
     */
    public List<List<Object>> getRows() {
        return rows;
    }

    /**
     * Whether the command addresses records by key and can be executed as part of an Aerospike batch.
     */
    public boolean isKeyed() {
        switch (query.getQueryType()) {
            case INSERT:
                return true;
            case UPDATE:
            case DELETE:
                return !query.getPrimaryKeys().isEmpty();
            default:
                return false;
        }
    }
}
