package com.aerospike.jdbc.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

public class TruncateQueryHandler extends BaseQueryHandler {

    private static final Logger logger = Logger.getLogger(TruncateQueryHandler.class.getName());

    public TruncateQueryHandler(IAerospikeClient client, Statement statement) {
        super(client, statement);
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        logger.info("TRUNCATE/DROP statement");
        client.truncate(null, query.getCatalog(), query.getSetName(), null);

        return new Pair<>(emptyRecordSet(query), 1);
    }
}
