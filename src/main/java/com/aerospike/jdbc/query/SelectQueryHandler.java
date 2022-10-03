package com.aerospike.jdbc.query;

import com.aerospike.client.BatchRead;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.BatchReadPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
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
import com.aerospike.jdbc.util.AerospikeUtils;
import com.aerospike.jdbc.util.VersionUtils;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.aerospike.jdbc.query.PolicyBuilder.buildBatchReadPolicy;
import static com.aerospike.jdbc.query.PolicyBuilder.buildQueryPolicy;
import static com.aerospike.jdbc.query.PolicyBuilder.buildScanNoBinDataPolicy;
import static com.aerospike.jdbc.query.PolicyBuilder.buildScanPolicy;
import static com.aerospike.jdbc.util.AerospikeUtils.getTableRecordsNumber;

public class SelectQueryHandler extends BaseQueryHandler {

    private static final Logger logger = Logger.getLogger(SelectQueryHandler.class.getName());

    protected List<DataColumn> columns;

    public SelectQueryHandler(IAerospikeClient client, Statement statement) {
        super(client, statement);
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        columns = AerospikeSchemaBuilder.getSchema(query.getSchemaTable(), client);
        Collection<Object> keyObjects = query.getPrimaryKeys();
        Optional<AerospikeSecondaryIndex> sIndex = secondaryIndex(query);
        Pair<ResultSet, Integer> result;
        if (isCount(query)) {
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
            ScanPolicy policy = buildScanNoBinDataPolicy(query);
            RecordSet recordSet = ScanQueryHandler.create(client).execute(policy, query);

            final AtomicInteger count = new AtomicInteger();
            recordSet.forEach(r -> count.incrementAndGet());
            recordNumber = count.get();
        }
        com.aerospike.client.Record aeroRecord = new com.aerospike.client.Record(Collections.singletonMap(
                countLabel, recordNumber), 1, 0);

        RecordSet recordSet = new RecordSet(2);
        recordSet.put(new KeyRecord(null, aeroRecord));
        recordSet.end();

        List<DataColumn> columnList = Collections.singletonList(new DataColumn(query.getSchema(),
                query.getTable(), Types.INTEGER, countLabel, countLabel));

        return new Pair<>(new AerospikeRecordResultSet(recordSet, statement, query.getSchema(),
                query.getTable(), columnList), -1);
    }

    private Pair<ResultSet, Integer> executeSelectByPrimaryKey(AerospikeQuery query, Collection<Object> keyObjects) {
        logger.info(() -> "SELECT primary key");
        final BatchReadPolicy policy = buildBatchReadPolicy(query);
        List<BatchRead> batchReadList = keyObjects.stream()
                .map(k -> new BatchRead(policy, new Key(query.getSchema(), query.getTable(), Value.get(k)), true))
                .collect(Collectors.toList());

        RecordSetBatchSequenceListener listener = new RecordSetBatchSequenceListener();
        client.get(EventLoopProvider.getEventLoop(), listener, null, batchReadList);

        return new Pair<>(new AerospikeRecordResultSet(listener.getRecordSet(), statement, query.getSchema(),
                query.getTable(), filterColumns(columns, query.getBinNames())), -1);
    }

    private Pair<ResultSet, Integer> executeScan(AerospikeQuery query) {
        logger.info(() -> "SELECT scan " + (Objects.nonNull(query.getOffset()) ? "partition" : "all"));

        ScanPolicy policy = buildScanPolicy(query);
        RecordSet recordSet = ScanQueryHandler.create(client).execute(policy, query);

        return new Pair<>(new AerospikeRecordResultSet(recordSet, statement, query.getSchema(),
                query.getTable(), filterColumns(columns, query.getBinNames())), -1);
    }

    private Pair<ResultSet, Integer> executeQuery(AerospikeQuery query,
                                                  AerospikeSecondaryIndex secondaryIndex) {
        logger.info(() -> "SELECT secondary index query for column: " + secondaryIndex.getBinName());

        QueryPolicy policy = buildQueryPolicy(query);
        RecordSet recordSet = SecondaryIndexQueryHandler.create(client).execute(policy, query, secondaryIndex);

        return new Pair<>(new AerospikeRecordResultSet(recordSet, statement, query.getSchema(),
                query.getTable(), filterColumns(columns, query.getBinNames())), -1);
    }

    private Optional<AerospikeSecondaryIndex> secondaryIndex(AerospikeQuery query) {
        if (VersionUtils.isSIndexSupported(client) && Objects.nonNull(query.getPredicate())
                && query.getPredicate().isIndexable() && Objects.isNull(query.getOffset())) {
            Map<String, AerospikeSecondaryIndex> indexMap = AerospikeUtils.getSecondaryIndexes(client);
            List<String> binNames = query.getPredicate().getBinNames();
            if (!binNames.isEmpty() && !indexMap.isEmpty()) {
                if (binNames.size() == 1) {
                    String binName = binNames.get(0);
                    for (AerospikeSecondaryIndex index : indexMap.values()) {
                        if (index.getBinName().equals(binName)) {
                            return Optional.of(index);
                        }
                    }
                } else {
                    List<AerospikeSecondaryIndex> indexList = new ArrayList<>(indexMap.values());
                    if (VersionUtils.isSIndexCardinalitySupported(client)) {
                        indexList.sort(Comparator.comparingInt(AerospikeSecondaryIndex::getBinValuesRatio));
                    } else {
                        indexList.sort(Comparator.comparing(AerospikeSecondaryIndex::getBinName));
                    }
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

    private boolean isCount(AerospikeQuery query) {
        return query.getColumns().size() == 1 &&
                query.getColumns().get(0).toLowerCase(Locale.ENGLISH).startsWith("count(");
    }

    private List<DataColumn> filterColumns(List<DataColumn> columns, String[] selected) {
        if (Objects.isNull(selected)) return columns;
        List<String> list = Arrays.stream(selected).collect(Collectors.toList());
        return columns.stream().filter(c -> list.contains(c.getName())).collect(Collectors.toList());
    }
}
