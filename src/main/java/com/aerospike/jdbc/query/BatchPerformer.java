package com.aerospike.jdbc.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchDelete;
import com.aerospike.client.Bin;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.jdbc.AerospikeConnection;
import com.aerospike.jdbc.async.EventLoopProvider;
import com.aerospike.jdbc.async.FutureBatchOperateListListener;
import com.aerospike.jdbc.async.FutureBatchRecordListener;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.DriverPolicy;
import com.aerospike.jdbc.util.AerospikeVersion;
import com.aerospike.jdbc.util.BatchUpdateCounts;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import static java.lang.String.format;

/**
 * Executes the commands of a JDBC batch in order.
 * <p>
 * Consecutive commands that address records by key (INSERT, and UPDATE/DELETE by primary key) are sent as
 * a single Aerospike batch. A group is flushed before a key repeats, so commands on the same record are
 * applied in order. Other commands (scan-based UPDATE/DELETE, TRUNCATE, CREATE/DROP INDEX) are executed
 * individually between the groups. Execution continues after a failure, so every command gets a result.
 */
public class BatchPerformer {

    private static final Logger logger = Logger.getLogger(BatchPerformer.class.getName());

    private final IAerospikeClient client;
    private final Statement statement;
    private final PolicyBuilder policyBuilder;
    private final DriverPolicy driverPolicy;
    private final AerospikeVersion aerospikeVersion;

    public BatchPerformer(IAerospikeClient client, Statement statement) throws SQLException {
        this.client = client;
        this.statement = statement;
        AerospikeConnection connection = (AerospikeConnection) statement.getConnection();
        policyBuilder = new PolicyBuilder(client);
        driverPolicy = connection.getConfiguration().getDriverPolicy();
        aerospikeVersion = connection.getAerospikeVersion();
    }

    public int[] execute(List<BatchCommand> commands) throws SQLException {
        List<List<BatchRecord>> commandRecords = buildRecords(commands);
        BatchUpdateCounts updateCounts = new BatchUpdateCounts(commands.size(),
                driverPolicy.getBatchSkipDuplicateKeys());

        Group group = new Group();
        for (int i = 0; i < commands.size(); i++) {
            BatchCommand command = commands.get(i);
            List<BatchRecord> records = commandRecords.get(i);
            if (records != null) {
                if (group.containsAnyKey(records)) {
                    flush(group, commands, updateCounts);
                    group = new Group();
                }
                group.add(i, records);
            } else {
                flush(group, commands, updateCounts);
                group = new Group();
                executeIndividually(i, command, updateCounts);
            }
        }
        flush(group, commands, updateCounts);

        return updateCounts.getCounts();
    }

    /**
     * Builds the batch records of every keyed command before anything is executed,
     * so that an invalid command does not leave the batch partially applied.
     */
    private List<List<BatchRecord>> buildRecords(List<BatchCommand> commands) throws SQLException {
        List<List<BatchRecord>> commandRecords = new ArrayList<>(commands.size());
        for (int i = 0; i < commands.size(); i++) {
            BatchCommand command = commands.get(i);
            try {
                commandRecords.add(command.isKeyed() ? buildRecords(command) : null);
            } catch (RuntimeException e) {
                throw new SQLException(format("Invalid batch command at index %d: %s", i, e.getMessage()), e);
            }
        }
        return commandRecords;
    }

    private List<BatchRecord> buildRecords(BatchCommand command) {
        AerospikeQuery query = command.getQuery();
        List<BatchRecord> records = new ArrayList<>();
        switch (command.getQueryType()) {
            case INSERT:
                List<String> binNames = InsertQueryHandler.getBinNames(query);
                BatchWritePolicy createOnlyPolicy = policyBuilder.buildBatchCreateOnlyPolicy();
                for (List<Object> row : command.getRows()) {
                    records.add(InsertQueryHandler.toBatchWrite(query, binNames, new ArrayList<>(row),
                            createOnlyPolicy));
                }
                break;

            case UPDATE:
                Operation[] operations = buildUpdateOperations(query);
                BatchWritePolicy updateOnlyPolicy = policyBuilder.buildBatchUpdateOnlyPolicy();
                for (Object primaryKey : query.getPrimaryKeys()) {
                    records.add(new BatchWrite(updateOnlyPolicy, buildKey(query, primaryKey), operations));
                }
                break;

            case DELETE:
                BatchDeletePolicy deletePolicy = policyBuilder.buildBatchDeletePolicy(query);
                for (Object primaryKey : query.getPrimaryKeys()) {
                    records.add(new BatchDelete(deletePolicy, buildKey(query, primaryKey)));
                }
                break;

            default:
                throw new IllegalArgumentException(format("Unexpected batch command type: %s",
                        command.getQueryType()));
        }
        return records;
    }

    private Operation[] buildUpdateOperations(AerospikeQuery query) {
        List<String> columns = query.getColumns();
        Operation[] operations = new Operation[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            operations[i] = Operation.put(new Bin(columns.get(i), Value.get(query.getValues().get(i))));
        }
        return operations;
    }

    private Key buildKey(AerospikeQuery query, Object primaryKey) {
        return new Key(query.getCatalog(), query.getSetName(), Value.get(primaryKey));
    }

    private void executeIndividually(int index, BatchCommand command, BatchUpdateCounts updateCounts) {
        logger.fine(() -> format("Batch command %d: %s", index, command.getQueryType()));
        try {
            updateCounts.setCount(index, QueryPerformer.executeQueryStrict(client, statement,
                    command.getQuery(), driverPolicy).getRight());
        } catch (RuntimeException e) {
            logger.warning(() -> format("Batch command %d failed: %s", index, e.getMessage()));
            updateCounts.setFailed(index, e);
        }
    }

    private void flush(Group group, List<BatchCommand> commands, BatchUpdateCounts updateCounts)
            throws SQLException {
        if (group.records.isEmpty()) {
            return;
        }
        AerospikeQuery query = commands.get(group.commandIndexes.get(0)).getQuery();
        logger.fine(() -> format("Batch of %d records for %d commands",
                group.records.size(), group.commandIndexes.size()));

        AerospikeException failure = aerospikeVersion.isBatchOpsSupported()
                ? operate(group.records, query)
                : operateIndividually(group.records, query);
        if (failure != null) {
            updateCounts.setBatchFailure(failure);
        }

        int offset = 0;
        for (int i = 0; i < group.commandIndexes.size(); i++) {
            int commandIndex = group.commandIndexes.get(i);
            int size = group.recordCounts.get(i);
            updateCounts.setRecordsResult(commandIndex, commands.get(commandIndex).getQueryType(),
                    group.records.subList(offset, offset + size));
            offset += size;
        }
    }

    /**
     * Executes the records as a single Aerospike batch.
     * The client sets the result of every record, also when the batch as a whole fails.
     *
     * @return the batch-level failure, or {@code null}
     */
    private AerospikeException operate(List<BatchRecord> records, AerospikeQuery query) throws SQLException {
        FutureBatchOperateListListener listener = new FutureBatchOperateListListener();
        try {
            client.operate(EventLoopProvider.getEventLoop(), listener,
                    policyBuilder.buildBatchPolicyDefault(query), records);
        } catch (AerospikeException e) {
            listener.onFailure(e);
        }
        return await(listener.getTotal());
    }

    /**
     * Emulates a batch with single-record commands for servers that do not support batch writes.
     */
    private AerospikeException operateIndividually(List<BatchRecord> records, AerospikeQuery query)
            throws SQLException {
        FutureBatchRecordListener listener = new FutureBatchRecordListener(records.size());
        for (BatchRecord batchRecord : records) {
            try {
                if (batchRecord instanceof BatchDelete) {
                    client.delete(EventLoopProvider.getEventLoop(), listener.deleteListener(batchRecord),
                            policyBuilder.buildWritePolicy(batchRecord, query), batchRecord.key);
                } else {
                    client.operate(EventLoopProvider.getEventLoop(), listener.writeListener(batchRecord),
                            policyBuilder.buildWritePolicy(batchRecord, query), batchRecord.key,
                            ((BatchWrite) batchRecord).ops);
                }
            } catch (AerospikeException e) {
                listener.onFailure(batchRecord, e);
            }
        }
        return await(listener.getCompletion());
    }

    private AerospikeException await(Future<?> future) throws SQLException {
        try {
            future.get();
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AerospikeException) {
                return (AerospikeException) e.getCause();
            }
            return new AerospikeException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting for the batch to complete", e);
        }
    }

    private static final class Group {
        private final List<BatchRecord> records = new ArrayList<>();
        private final List<Integer> commandIndexes = new ArrayList<>();
        private final List<Integer> recordCounts = new ArrayList<>();
        private final Set<Key> keys = new HashSet<>();

        boolean containsAnyKey(List<BatchRecord> commandRecords) {
            for (BatchRecord batchRecord : commandRecords) {
                if (keys.contains(batchRecord.key)) {
                    return true;
                }
            }
            return false;
        }

        void add(int commandIndex, List<BatchRecord> commandRecords) {
            records.addAll(commandRecords);
            commandIndexes.add(commandIndex);
            recordCounts.add(commandRecords.size());
            for (BatchRecord batchRecord : commandRecords) {
                keys.add(batchRecord.key);
            }
        }
    }
}
