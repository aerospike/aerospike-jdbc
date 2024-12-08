package com.aerospike.jdbc.query;

import com.aerospike.client.*;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.async.EventLoopProvider;
import com.aerospike.jdbc.async.FutureBatchOperateListListener;
import com.aerospike.jdbc.async.FutureWriteListener;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;

public class InsertQueryHandler extends BaseQueryHandler {

    private static final Logger logger = Logger.getLogger(InsertQueryHandler.class.getName());

    public InsertQueryHandler(IAerospikeClient client, Statement statement) {
        super(client, statement);
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        if (aerospikeVersion.isBatchOpsSupported()) {
            logger.info("INSERT batch");
            return putBatch(query);
        }
        logger.info("INSERT individual");
        return putConsecutively(query);
    }

    public Pair<ResultSet, Integer> putConsecutively(AerospikeQuery query) {
        List<String> binNames = getBinNames(query);

        FutureWriteListener listener = new FutureWriteListener(query.getValues().size());
        WritePolicy writePolicy = policyBuilder.buildCreateOnlyPolicy(query);

        for (Object aerospikeRecord : query.getValues()) {
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) aerospikeRecord;
            Value recordKey = extractInsertKey(query, values);
            Key key = new Key(query.getCatalog(), query.getSetName(), recordKey);
            Bin[] bins = buildBinArray(binNames, values);

            try {
                client.put(EventLoopProvider.getEventLoop(), listener, writePolicy, key, bins);
            } catch (AerospikeException e) {
                logAerospikeException(e);
                listener.onFailure(e);
            }
        }
        return new Pair<>(emptyRecordSet(query), getUpdateCount(listener.getTotal()));
    }

    public Pair<ResultSet, Integer> putBatch(AerospikeQuery query) {
        List<String> binNames = getBinNames(query);

        FutureBatchOperateListListener listener = new FutureBatchOperateListListener();
        List<BatchRecord> batchRecords = new ArrayList<>();
        BatchWritePolicy batchWritePolicy = policyBuilder.buildBatchCreateOnlyPolicy();

        for (Object aerospikeRecord : query.getValues()) {
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) aerospikeRecord;
            Value recordKey = extractInsertKey(query, values);
            Key key = new Key(query.getCatalog(), query.getSetName(), recordKey);
            batchRecords.add(
                    new BatchWrite(
                            batchWritePolicy,
                            key,
                            Arrays.stream(buildBinArray(binNames, values))
                                    .map(Operation::put)
                                    .toArray(Operation[]::new)
                    )
            );
        }
        BatchPolicy batchPolicy = policyBuilder.buildBatchPolicyDefault(query);
        try {
            client.operate(EventLoopProvider.getEventLoop(), listener, batchPolicy, batchRecords);
        } catch (AerospikeException e) {
            // no error log as this completes the future exceptionally
            listener.onFailure(e);
        }

        return new Pair<>(emptyRecordSet(query), getUpdateCount(listener.getTotal()));
    }

    protected Bin[] buildBinArray(List<String> binNames, List<Object> values) {
        Bin[] bins = new Bin[binNames.size()];
        for (int i = 0; i < binNames.size(); i++) {
            bins[i] = new Bin(binNames.get(i), Value.get(values.get(i)));
        }
        return bins;
    }

    private List<String> getBinNames(AerospikeQuery query) {
        return query.getColumns().stream()
                .filter(c -> !c.equals(PRIMARY_KEY_COLUMN_NAME))
                .collect(Collectors.toList());
    }

    private Value extractInsertKey(AerospikeQuery query, List<Object> values) {
        List<String> columns = query.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(PRIMARY_KEY_COLUMN_NAME)) {
                Object key = values.get(i);
                values.remove(i);
                if (key == null) break;
                return Value.get(key);
            }
        }
        return Value.get(UUID.randomUUID().toString());
    }
}
