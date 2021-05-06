package com.aerospike.jdbc.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;
import com.aerospike.jdbc.scan.EventLoopProvider;
import com.aerospike.jdbc.scan.ScanRecordSequenceListener;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Objects;
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
        logger.info("UPDATE statement");
        Object keyObject = ExpressionBuilder.fetchPrimaryKey(query.getWhere());
        final Bin[] bins = getBins(query);
        final WritePolicy writePolicy = buildUpdateOnlyPolicy(query);
        if (Objects.nonNull(keyObject)) {
            Key key = new Key(query.getSchema(), query.getTable(), getBinValue(keyObject.toString()));
            try {
                client.put(writePolicy, key, bins);
            } catch (AerospikeException e) {
                return new Pair<>(emptyRecordSet(query), 0);
            }

            return new Pair<>(emptyRecordSet(query), 1);
        } else {
            ScanRecordSequenceListener listener = new ScanRecordSequenceListener();
            ScanPolicy scanPolicy = buildScanPolicy(query);
            scanPolicy.includeBinData = false;
            client.scanAll(EventLoopProvider.getEventLoop(), listener, scanPolicy, query.getSchema(),
                    query.getTable());

            final AtomicInteger count = new AtomicInteger();
            listener.getRecordSet().forEach(r -> {
                try {
                    client.put(writePolicy, r.key, bins);
                    count.incrementAndGet();
                } catch (AerospikeException ignore) {
                }
            });

            return new Pair<>(emptyRecordSet(query), count.get());
        }
    }
}
