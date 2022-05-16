package com.aerospike.jdbc.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.async.EventLoopProvider;
import com.aerospike.jdbc.async.FutureDeleteListener;
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
import static com.aerospike.jdbc.query.PolicyBuilder.buildWritePolicy;

public class DeleteQueryHandler extends BaseQueryHandler {

    private static final Logger logger = Logger.getLogger(DeleteQueryHandler.class.getName());

    public DeleteQueryHandler(IAerospikeClient client, Statement statement) {
        super(client, statement);
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        Collection<Object> keyObjects = query.getPrimaryKeys();
        final WritePolicy writePolicy = buildWritePolicy(query);
        if (!keyObjects.isEmpty()) {
            logger.info("DELETE primary key");
            FutureDeleteListener listener = new FutureDeleteListener(keyObjects.size());
            for (Object keyObject : keyObjects) {
                Key key = new Key(query.getSchema(), query.getTable(), Value.get(keyObject));
                try {
                    client.delete(EventLoopProvider.getEventLoop(), listener, writePolicy, key);
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
            logger.info("DELETE scan");
            RecordSetRecordSequenceListener listener = new RecordSetRecordSequenceListener();
            ScanPolicy scanPolicy = buildScanPolicy(query);
            scanPolicy.includeBinData = false;
            client.scanAll(EventLoopProvider.getEventLoop(), listener, scanPolicy, query.getSchema(),
                    query.getTable());

            final AtomicInteger count = new AtomicInteger();
            listener.getRecordSet().forEach(r -> {
                try {
                    if (client.delete(writePolicy, r.key))
                        count.incrementAndGet();
                } catch (Exception e) {
                    logger.warning("Failed to delete record: " + e.getMessage());
                }
            });

            return new Pair<>(emptyRecordSet(query), count.get());
        }
    }
}
