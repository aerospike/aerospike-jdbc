package com.aerospike.jdbc.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.DriverPolicy;
import com.aerospike.jdbc.model.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.aerospike.jdbc.util.Constants.UNSUPPORTED_QUERY_TYPE_MESSAGE;

public final class QueryPerformer {

    private QueryPerformer() {
    }

    public static Pair<ResultSet, Integer> executeQuery(
            IAerospikeClient client,
            Statement statement,
            AerospikeQuery query,
            DriverPolicy driverPolicy
    ) {
        return createHandler(client, statement, query, driverPolicy).execute(query);
    }

    /**
     * Executes a query, rethrowing failures that {@link #executeQuery} only logs.
     */
    public static Pair<ResultSet, Integer> executeQueryStrict(
            IAerospikeClient client,
            Statement statement,
            AerospikeQuery query,
            DriverPolicy driverPolicy
    ) {
        BaseQueryHandler queryHandler = createHandler(client, statement, query, driverPolicy);
        queryHandler.setStrict(true);
        return queryHandler.execute(query);
    }

    public static int[] executeBatch(
            IAerospikeClient client,
            Statement statement,
            List<BatchCommand> commands
    ) throws SQLException {
        return new BatchPerformer(client, statement).execute(commands);
    }

    private static BaseQueryHandler createHandler(
            IAerospikeClient client,
            Statement statement,
            AerospikeQuery query,
            DriverPolicy driverPolicy
    ) {
        switch (query.getQueryType()) {
            case SELECT:
                return new SelectQueryHandler(client, statement, driverPolicy);

            case INSERT:
                return new InsertQueryHandler(client, statement);

            case UPDATE:
                return new UpdateQueryHandler(client, statement);

            case DELETE:
                return new DeleteQueryHandler(client, statement);

            case DROP_TABLE:
                return new TruncateQueryHandler(client, statement);

            case CREATE_INDEX:
                return new IndexCreateHandler(client, statement);

            case DROP_INDEX:
                return new IndexDropHandler(client, statement);

            case EXPLAIN:
                return new ExplainQueryHandler(client, statement, driverPolicy);

            default:
                throw new UnsupportedOperationException(UNSUPPORTED_QUERY_TYPE_MESSAGE);
        }
    }
}
