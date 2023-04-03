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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static com.aerospike.jdbc.query.PolicyBuilder.buildScanPolicy;
import static com.aerospike.jdbc.query.PolicyBuilder.buildUpdateOnlyPolicy;

public class UpdateQueryHandler extends BaseQueryHandler {

    private static final Logger logger = Logger.getLogger(UpdateQueryHandler.class.getName());

    public UpdateQueryHandler(IAerospikeClient client, Statement statement) {
        super(client, statement);
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        Collection<Object> keyObjects = query.getPrimaryKeys();
        final Bin[] bins = getBins(query);
        final WritePolicy writePolicy = buildUpdateOnlyPolicy();
        if (!keyObjects.isEmpty()) {
            logger.info("UPDATE primary key");
            FutureWriteListener listener = new FutureWriteListener(keyObjects.size());
            for (Object keyObject : keyObjects) {
                Key key = new Key(query.getSchema(), query.getSetName(), Value.get(keyObject));
                try {
                    client.put(EventLoopProvider.getEventLoop(), listener, writePolicy, key, bins);
                } catch (AerospikeException e) {
                    logger.warning("Error on database call: " + e.getMessage());
                    listener.onFailure(e);
                }
            }
            try {
                return new Pair<>(emptyRecordSet(query), listener.getTotal().get());
            } catch (InterruptedException | ExecutionException e) {
                return new Pair<>(emptyRecordSet(query), 0);
            }
        } else {
            logger.info("UPDATE scan");
            RecordSetRecordSequenceListener listener = new RecordSetRecordSequenceListener();
            ScanPolicy scanPolicy = buildScanPolicy(query);
            scanPolicy.includeBinData = false;
            client.scanAll(EventLoopProvider.getEventLoop(), listener, scanPolicy, query.getSchema(),
                    query.getSetName());

            final AtomicInteger count = new AtomicInteger();
            listener.getRecordSet().forEach(r -> {
                try {
                    client.put(writePolicy, r.key, bins);
                    count.incrementAndGet();
                } catch (AerospikeException e) {
                    logger.warning("Failed to update record: " + e.getMessage());
                }
            });

            return new Pair<>(emptyRecordSet(query), count.get());
        }
    }
}
