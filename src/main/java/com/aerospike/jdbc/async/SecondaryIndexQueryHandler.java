package com.aerospike.jdbc.async;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.AerospikeSecondaryIndex;
import com.aerospike.jdbc.util.URLParser;

import java.util.Objects;
import java.util.Optional;
import java.util.logging.Logger;

public class SecondaryIndexQueryHandler {

    private static final Logger logger = Logger.getLogger(ScanQueryHandler.class.getName());

    private final IAerospikeClient client;
    private final RecordSetRecordSequenceListener listener;

    public SecondaryIndexQueryHandler(IAerospikeClient client) {
        this.client = client;
        this.listener = new RecordSetRecordSequenceListener();
    }

    public RecordSet execute(QueryPolicy queryPolicy, AerospikeQuery query,
                             AerospikeSecondaryIndex secondaryIndex) {
        com.aerospike.client.query.Statement statement = new com.aerospike.client.query.Statement();
        Optional.ofNullable(query.getLimit()).ifPresent(statement::setMaxRecords);
        statement.setRecordsPerSecond(URLParser.getScanPolicy().recordsPerSecond);

        statement.setIndexName(secondaryIndex.getIndexName());
        statement.setNamespace(query.getSchema());
        statement.setSetName(query.getTable());
        if (Objects.nonNull(query.getPredicate())) {
            query.getPredicate().toFilter(secondaryIndex.getBinName()).ifPresent(statement::setFilter);
        }

        client.query(EventLoopProvider.getEventLoop(), listener, queryPolicy, statement);

        return listener.getRecordSet();
    }

    public static SecondaryIndexQueryHandler create(IAerospikeClient client) {
        return new SecondaryIndexQueryHandler(client);
    }
}
