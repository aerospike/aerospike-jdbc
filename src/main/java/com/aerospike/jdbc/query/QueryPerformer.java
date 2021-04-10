package com.aerospike.jdbc.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;

import java.sql.ResultSet;
import java.sql.Statement;

public final class QueryPerformer {

    public static Pair<ResultSet, Integer> executeQuery(IAerospikeClient client,
                                                        Statement statement, AerospikeQuery query) {

        QueryHandler queryHandler;
        switch (query.getType()) {
            case SELECT:
                queryHandler = new SelectQueryHandler(client, statement);
                return queryHandler.execute(query);

            case INSERT:
                queryHandler = new InsertQueryHandler(client, statement);
                return queryHandler.execute(query);

            case UPDATE:
                queryHandler = new UpdateQueryHandler(client, statement);
                return queryHandler.execute(query);

            case DELETE:
                queryHandler = new DeleteQueryHandler(client, statement);
                return queryHandler.execute(query);

            case DROP_TABLE:
                queryHandler = new TruncateQueryHandler(client, statement);
                return queryHandler.execute(query);

            default:
                throw new RuntimeException("Unsupported query type");
        }
    }

}
