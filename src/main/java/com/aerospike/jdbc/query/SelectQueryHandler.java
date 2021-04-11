package com.aerospike.jdbc.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.DataColumn;
import com.aerospike.jdbc.model.Pair;
import com.aerospike.jdbc.scan.PartitionScanHandler;
import com.aerospike.jdbc.scan.RecordSet;
import com.aerospike.jdbc.schema.AerospikeSchemaBuilder;
import com.aerospike.jdbc.sql.AerospikeRecordResultSet;
import com.aerospike.jdbc.util.IOUtils;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
        Value pk = ExpressionBuilder.fetchPrimaryKey(query.getWhere());
        Pair<ResultSet, Integer> result;
        if (isCount(query)) {
            result = executeCountQuery(query);
        } else if (Objects.nonNull(pk)) {
            result = executeSelectByPrimaryKey(query, pk);
        } else {
            result = executeScanQuery(query);
        }

        return result;
    }

    private Pair<ResultSet, Integer> executeCountQuery(AerospikeQuery query) {
        logger.info("SELECT count");
        String countLabel = query.getColumns().get(0);
        int recordNumber;
        if (Objects.isNull(query.getWhere())) {
            recordNumber = getTableRecordsNumber(client, query.getSchema(), query.getTable());
        } else {
            ScanPolicy policy = buildScanNoBinDataPolicy(query);
            RecordSet recordSet = PartitionScanHandler.create(client).scanPartition(policy, query);

            final AtomicInteger count = new AtomicInteger();
            recordSet.forEach(r -> count.incrementAndGet());
            recordNumber = count.get();
        }
        com.aerospike.client.Record record = new com.aerospike.client.Record(Collections.singletonMap(
                countLabel, recordNumber), 1, 0);

        RecordSet recordSet = new RecordSet(2);
        recordSet.put(new KeyRecord(null, record));
        recordSet.end();

        List<DataColumn> columnList = Collections.singletonList(new DataColumn(query.getSchema(),
                query.getTable(), Types.INTEGER, countLabel, countLabel));

        return new Pair<>(new AerospikeRecordResultSet(recordSet, statement, query.getSchema(),
                query.getTable(), columnList), -1);
    }

    private Pair<ResultSet, Integer> executeSelectByPrimaryKey(AerospikeQuery query, Value primaryKey) {
        logger.info("SELECT PK");
        Key key = new Key(query.getSchema(), query.getTable(), primaryKey);
        com.aerospike.client.Record record = client.get(null, key, query.getBinNames());

        RecordSet recordSet;
        if (Objects.nonNull(record)) {
            recordSet = new RecordSet(2);
            KeyRecord keyRecord = new KeyRecord(key, record);
            recordSet.put(keyRecord);
        } else {
            recordSet = new RecordSet(1);
        }
        recordSet.end();

        return new Pair<>(new AerospikeRecordResultSet(recordSet, statement, query.getSchema(),
                query.getTable(), filterColumns(columns, query.getBinNames())), -1);
    }

    private Pair<ResultSet, Integer> executeScanQuery(AerospikeQuery query) {
        logger.info("SELECT scan");

        ScanPolicy policy = buildScanPolicy(query);
        RecordSet recordSet = PartitionScanHandler.create(client).scanPartition(policy, query);

        return new Pair<>(new AerospikeRecordResultSet(recordSet, statement, query.getSchema(),
                query.getTable(), filterColumns(columns, query.getBinNames())), -1);
    }

    private boolean isCount(AerospikeQuery query) {
        return query.getColumns().size() == 1 &&
                query.getColumns().get(0).toLowerCase(Locale.ENGLISH).startsWith("count(");
    }

    private List<DataColumn> filterColumns(List<DataColumn> columns, String[] selected) {
        if (Objects.isNull(selected)) return columns;
        List<String> list = Arrays.stream(selected).map(IOUtils::stripQuotes).collect(Collectors.toList());
        return columns.stream().filter(c -> list.contains(c.getName())).collect(Collectors.toList());
    }

}
