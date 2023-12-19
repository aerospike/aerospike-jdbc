package com.aerospike.jdbc.async;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.AerospikeSecondaryIndex;
import com.aerospike.jdbc.model.DriverPolicy;

import java.util.Objects;
import java.util.Optional;

public class SecondaryIndexQueryHandler {

    private final IAerospikeClient client;
    private final RecordSetRecordSequenceListener listener;

    public SecondaryIndexQueryHandler(IAerospikeClient client, DriverPolicy driverPolicy) {
        this.client = client;
        this.listener = new RecordSetRecordSequenceListener(driverPolicy);
    }

    public static SecondaryIndexQueryHandler create(IAerospikeClient client, DriverPolicy driverPolicy) {
        return new SecondaryIndexQueryHandler(client, driverPolicy);
    }

    public RecordSet execute(QueryPolicy queryPolicy, AerospikeQuery query,
                             AerospikeSecondaryIndex secondaryIndex) {
        com.aerospike.client.query.Statement statement = new com.aerospike.client.query.Statement();
        Optional.ofNullable(query.getLimit()).ifPresent(statement::setMaxRecords);
        statement.setRecordsPerSecond(client.getScanPolicyDefault().recordsPerSecond);

        statement.setIndexName(secondaryIndex.getIndexName());
        statement.setNamespace(query.getCatalog());
        statement.setSetName(query.getTable());
        statement.setBinNames(query.columnBins());

        if (Objects.nonNull(query.getPredicate())) {
            query.getPredicate().toFilter(secondaryIndex.getBinName()).ifPresent(statement::setFilter);
        }

        if (query.isPrimaryKeyOnly()) {
            queryPolicy.includeBinData = false;
        }
        client.query(EventLoopProvider.getEventLoop(), listener, queryPolicy, statement);

        return listener.getRecordSet();
    }
}
