package com.aerospike.jdbc.query;

import com.aerospike.client.*;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.async.EventLoopProvider;
import com.aerospike.jdbc.async.FutureBatchOperateListListener;
import com.aerospike.jdbc.async.FutureWriteListener;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;
import com.aerospike.jdbc.util.VersionUtils;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.aerospike.jdbc.query.PolicyBuilder.buildCreateOnlyPolicy;
import static com.aerospike.jdbc.util.Constants.defaultKeyName;

public class InsertQueryHandler extends BaseQueryHandler {

    private static final Logger logger = Logger.getLogger(InsertQueryHandler.class.getName());

    public InsertQueryHandler(IAerospikeClient client, Statement statement) {
        super(client, statement);
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        if (VersionUtils.isBatchOpsSupported(client)) {
            logger.info("INSERT batch");
            return putBatch(query);
        }
        logger.info("INSERT individual");
        return putConsecutively(query);
    }

    public Pair<ResultSet, Integer> putConsecutively(AerospikeQuery query) {
        List<String> binNames = query.getColumns().stream().filter(c -> !c.equals(defaultKeyName))
                .collect(Collectors.toList());

        FutureWriteListener listener = new FutureWriteListener(query.getValues().size());
        WritePolicy writePolicy = buildCreateOnlyPolicy();
        for (Object record : query.getValues()) {
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) record;
            Value recordKey = extractInsertKey(query, values);
            Key key = new Key(query.getSchema(), query.getTable(), recordKey);
            Bin[] bins = buildBinArray(binNames, values);

            try {
                client.put(EventLoopProvider.getEventLoop(), listener, writePolicy, key, bins);
            } catch (AerospikeException e) {
                logger.severe("Error on database call: " + e.getMessage());
                listener.onFailure(e);
            }
        }
        try {
            return new Pair<>(emptyRecordSet(query), listener.getTotal().get());
        } catch (InterruptedException | ExecutionException e) {
            return new Pair<>(emptyRecordSet(query), 0);
        }
    }

    public Pair<ResultSet, Integer> putBatch(AerospikeQuery query) {
        List<String> binNames = query.getColumns().stream().filter(c -> !c.equals(defaultKeyName))
                .collect(Collectors.toList());

        FutureBatchOperateListListener listener = new FutureBatchOperateListListener();
        List<BatchRecord> batchRecords = new ArrayList<>();

        BatchWritePolicy batchWritePolicy = new BatchWritePolicy();
        batchWritePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        batchWritePolicy.sendKey = true;

        for (Object record : query.getValues()) {
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) record;
            Value recordKey = extractInsertKey(query, values);
            Key key = new Key(query.getSchema(), query.getTable(), recordKey);
            batchRecords.add(
                    new BatchWrite(
                            batchWritePolicy,
                            key,
                            Arrays.stream(buildBinArray(binNames, values)).map(Operation::put).toArray(Operation[]::new)
                    )
            );
        }
        BatchPolicy batchPolicy = new BatchPolicy();
        batchPolicy.sendKey = true;
        batchPolicy.maxConcurrentThreads = batchRecords.size() / 100 + 1;

        try {
            client.operate(EventLoopProvider.getEventLoop(), listener, batchPolicy, batchRecords);
        } catch (AerospikeException e) {
            logger.severe("Error on database call: " + e.getMessage());
            listener.onFailure(e);
        }

        try {
            return new Pair<>(emptyRecordSet(query), listener.getTotal().get());
        } catch (InterruptedException | ExecutionException e) {
            return new Pair<>(emptyRecordSet(query), 0);
        }
    }

    protected Bin[] buildBinArray(List<String> binNames, List<Object> values) {
        Bin[] bins = new Bin[binNames.size()];
        for (int i = 0; i < binNames.size(); i++) {
            bins[i] = new Bin(binNames.get(i), values.get(i));
        }
        return bins;
    }

    private Value extractInsertKey(AerospikeQuery query, List<Object> values) {
        List<String> columns = query.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(defaultKeyName)) {
                Object key = values.get(i);
                values.remove(i);
                if (key == null) break;
                return Value.get(key);
            }
        }
        return Value.get(UUID.randomUUID().toString());
    }
}
