package com.aerospike.jdbc.async;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.AerospikeSecondaryIndex;
import com.aerospike.jdbc.model.DriverConfiguration;

import java.util.Objects;
import java.util.Optional;

public class SecondaryIndexQueryHandler {

    private final IAerospikeClient client;
    private final DriverConfiguration config;
    private final RecordSetRecordSequenceListener listener;

    public SecondaryIndexQueryHandler(IAerospikeClient client, DriverConfiguration config) {
        this.client = client;
        this.config = config;
        this.listener = new RecordSetRecordSequenceListener(config.getDriverPolicy());
    }

    public static SecondaryIndexQueryHandler create(IAerospikeClient client, DriverConfiguration config) {
        return new SecondaryIndexQueryHandler(client, config);
    }

    public RecordSet execute(QueryPolicy queryPolicy, AerospikeQuery query,
                             AerospikeSecondaryIndex secondaryIndex) {
        com.aerospike.client.query.Statement statement = new com.aerospike.client.query.Statement();
        Optional.ofNullable(query.getLimit()).ifPresent(statement::setMaxRecords);
        statement.setRecordsPerSecond(config.getScanPolicy().recordsPerSecond);

        statement.setIndexName(secondaryIndex.getIndexName());
        statement.setNamespace(query.getSchema());
        statement.setSetName(query.getTable());
        if (Objects.nonNull(query.getPredicate())) {
            query.getPredicate().toFilter(secondaryIndex.getBinName()).ifPresent(statement::setFilter);
        }

        client.query(EventLoopProvider.getEventLoop(), listener, queryPolicy, statement);

        return listener.getRecordSet();
    }
}
