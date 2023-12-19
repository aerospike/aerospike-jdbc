package com.aerospike.jdbc.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

public class IndexDropHandler extends BaseQueryHandler {

    private static final Logger logger = Logger.getLogger(IndexDropHandler.class.getName());

    protected IndexDropHandler(IAerospikeClient client, Statement statement) {
        super(client, statement);
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        logger.info("DROP INDEX statement");
        client.dropIndex(null, query.getCatalog(), query.getTable(), query.getIndex());

        databaseMetadata.resetCatalogIndexes();
        return new Pair<>(emptyRecordSet(query), 1);
    }
}
