package com.aerospike.jdbc.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.async.EventLoopProvider;
import com.aerospike.jdbc.async.FutureWriteListener;
import com.aerospike.jdbc.async.RecordSetRecordSequenceListener;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class UpdateQueryHandler extends BaseQueryHandler {

    private static final Logger logger = Logger.getLogger(UpdateQueryHandler.class.getName());

    public UpdateQueryHandler(IAerospikeClient client, Statement statement) {
        super(client, statement);
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        Collection<Object> keyObjects = query.getPrimaryKeys();
        final Bin[] bins = getBins(query);
        final WritePolicy writePolicy = policyBuilder.buildUpdateOnlyPolicy(query);
        if (!keyObjects.isEmpty()) {
            logger.info("UPDATE primary key");
            FutureWriteListener listener = new FutureWriteListener(keyObjects.size());
            for (Object keyObject : keyObjects) {
                Key key = new Key(query.getCatalog(), query.getSetName(), Value.get(keyObject));
                try {
                    client.put(EventLoopProvider.getEventLoop(), listener, writePolicy, key, bins);
                } catch (AerospikeException e) {
                    logAerospikeException(e);
                    listener.onFailure(e);
                }
            }
            return new Pair<>(emptyRecordSet(query), getUpdateCount(listener.getTotal()));
        } else {
            logger.info("UPDATE scan");
            RecordSetRecordSequenceListener listener = new RecordSetRecordSequenceListener(config.getDriverPolicy());
            ScanPolicy scanPolicy = policyBuilder.buildScanPolicy(query);
            scanPolicy.includeBinData = false;
            client.scanAll(EventLoopProvider.getEventLoop(), listener, scanPolicy, query.getCatalog(),
                    query.getSetName());

            final AtomicInteger count = new AtomicInteger();
            listener.getRecordSet().forEach(r -> {
                try {
                    client.put(writePolicy, r.key, bins);
                    count.incrementAndGet();
                } catch (AerospikeException e) {
                    logAerospikeException(e);
                }
            });

            return new Pair<>(emptyRecordSet(query), count.get());
        }
    }
}
