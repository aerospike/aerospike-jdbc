package com.aerospike.jdbc.query;

import com.aerospike.client.BatchRead;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.BatchReadPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.jdbc.AerospikeDatabaseMetadata;
import com.aerospike.jdbc.async.EventLoopProvider;
import com.aerospike.jdbc.async.RecordSet;
import com.aerospike.jdbc.async.RecordSetBatchSequenceListener;
import com.aerospike.jdbc.async.ScanQueryHandler;
import com.aerospike.jdbc.async.SecondaryIndexQueryHandler;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.AerospikeSecondaryIndex;
import com.aerospike.jdbc.model.DataColumn;
import com.aerospike.jdbc.model.Pair;
import com.aerospike.jdbc.schema.AerospikeSchemaBuilder;
import com.aerospike.jdbc.sql.AerospikeRecordResultSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.aerospike.jdbc.util.AerospikeUtils.getTableRecordsNumber;

public class SelectQueryHandler extends BaseQueryHandler {

    private static final Logger logger = Logger.getLogger(SelectQueryHandler.class.getName());

    protected final Map<String, AerospikeSecondaryIndex> secondaryIndexes;
    protected List<DataColumn> columns;

    public SelectQueryHandler(IAerospikeClient client, Statement statement) {
        super(client, statement);
        try {
            secondaryIndexes = ((AerospikeDatabaseMetadata) statement.getConnection().getMetaData())
                    .getSecondaryIndexes();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to get secondary indexes", e);
        }
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        columns = AerospikeSchemaBuilder.getSchema(query.getSchemaTable(), client);
        Collection<Object> keyObjects = query.getPrimaryKeys();
        Optional<AerospikeSecondaryIndex> sIndex = secondaryIndex(query);
        Pair<ResultSet, Integer> result;
        if (query.isCount()) {
            result = executeCountQuery(query);
        } else if (!keyObjects.isEmpty()) {
            result = executeSelectByPrimaryKey(query, keyObjects);
        } else {
            result = sIndex.map(secondaryIndex -> executeQuery(query, secondaryIndex))
                    .orElseGet(() -> executeScan(query));
        }
        return result;
    }

    private Pair<ResultSet, Integer> executeCountQuery(AerospikeQuery query) {
        logger.info(() -> "SELECT count");
        String countLabel = query.getColumns().get(0);
        int recordNumber;
        if (Objects.isNull(query.getPredicate())) {
            recordNumber = getTableRecordsNumber(client, query.getSchema(), query.getTable());
        } else {
            ScanPolicy policy = policyBuilder.buildScanNoBinDataPolicy(query);
            RecordSet recordSet = ScanQueryHandler.create(client, config.getDriverPolicy())
                    .execute(policy, query);

            final AtomicInteger count = new AtomicInteger();
            recordSet.forEach(r -> count.incrementAndGet());
            recordNumber = count.get();
        }
        com.aerospike.client.Record aeroRecord = new com.aerospike.client.Record(Collections.singletonMap(
                countLabel, recordNumber), 1, 0);

        RecordSet recordSet = new RecordSet(2, config.getDriverPolicy().getRecordSetTimeoutMs());
        recordSet.put(new KeyRecord(null, aeroRecord));
        recordSet.close();

        columns = Collections.singletonList(new DataColumn(query.getSchema(), query.getTable(),
                Types.INTEGER, countLabel, countLabel));

        return queryResult(recordSet, query);
    }

    private Pair<ResultSet, Integer> executeSelectByPrimaryKey(AerospikeQuery query, Collection<Object> keyObjects) {
        logger.info(() -> "SELECT primary key");
        final BatchReadPolicy policy = policyBuilder.buildBatchReadPolicy(query);
        List<BatchRead> batchReadList = keyObjects.stream()
                .map(k -> {
                    Key key = new Key(query.getSchema(), query.getSetName(), Value.get(k));
                    return new BatchRead(policy, key, true);
                })
                .collect(Collectors.toList());

        RecordSetBatchSequenceListener listener = new RecordSetBatchSequenceListener(config.getDriverPolicy());
        client.get(EventLoopProvider.getEventLoop(), listener, null, batchReadList);

        return queryResult(listener.getRecordSet(), query);
    }

    private Pair<ResultSet, Integer> executeScan(AerospikeQuery query) {
        logger.info(() -> "SELECT scan " + (Objects.nonNull(query.getOffset()) ? "partition" : "all"));

        ScanPolicy policy = policyBuilder.buildScanPolicy(query);
        RecordSet recordSet = ScanQueryHandler.create(client, config.getDriverPolicy()).execute(policy, query);

        return queryResult(recordSet, query);
    }

    private Pair<ResultSet, Integer> executeQuery(AerospikeQuery query,
                                                  AerospikeSecondaryIndex secondaryIndex) {
        logger.info(() -> "SELECT secondary index query for column: " + secondaryIndex.getBinName());

        QueryPolicy policy = policyBuilder.buildQueryPolicy(query);
        RecordSet recordSet = SecondaryIndexQueryHandler.create(client, config.getDriverPolicy())
                .execute(policy, query, secondaryIndex);

        return queryResult(recordSet, query);
    }

    private Optional<AerospikeSecondaryIndex> secondaryIndex(AerospikeQuery query) {
        if (aerospikeVersion.isSIndexSupported() && query.isIndexable()) {
            Map<String, AerospikeSecondaryIndex> indexMap = secondaryIndexes;
            List<String> binNames = query.getPredicate().getBinNames();
            if (!binNames.isEmpty() && indexMap != null && !indexMap.isEmpty()) {
                if (binNames.size() == 1) {
                    String binName = binNames.get(0);
                    for (AerospikeSecondaryIndex index : indexMap.values()) {
                        if (index.getBinName().equals(binName)) {
                            return Optional.of(index);
                        }
                    }
                } else {
                    List<AerospikeSecondaryIndex> indexList = new ArrayList<>(indexMap.values());
                    sortIndexList(indexList);
                    for (AerospikeSecondaryIndex index : indexList) {
                        if (binNames.contains(index.getBinName())) {
                            return Optional.of(index);
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }

    private Pair<ResultSet, Integer> queryResult(RecordSet recordSet, AerospikeQuery query) {
        return new Pair<>(new AerospikeRecordResultSet(recordSet, statement, query.getSchema(),
                query.getTable(), filterColumns(query)), -1);
    }

    private void sortIndexList(List<AerospikeSecondaryIndex> indexList) {
        if (aerospikeVersion.isSIndexCardinalitySupported()) {
            indexList.sort(Comparator.comparingInt(AerospikeSecondaryIndex::getBinValuesRatio));
        } else {
            indexList.sort(Comparator.comparing(AerospikeSecondaryIndex::getBinName));
        }
    }

    private List<DataColumn> filterColumns(AerospikeQuery query) {
        if (query.isStar()) {
            return columns;
        }
        return columns.stream()
                .filter(c -> query.getColumns().contains(c.getName()))
                .collect(Collectors.toList());
    }
}
