package com.aerospike.jdbc.query;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.DataColumn;
import com.aerospike.jdbc.model.Pair;
import com.aerospike.jdbc.scan.EventLoopProvider;
import com.aerospike.jdbc.scan.RecordSet;
import com.aerospike.jdbc.scan.ScanRecordSequenceListener;
import com.aerospike.jdbc.schema.AerospikeSchemaBuilder;
import com.aerospike.jdbc.sql.AerospikeRecordResultSet;
import com.aerospike.jdbc.sql.ListRecordSet;
import com.aerospike.jdbc.util.IOUtils;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.aerospike.jdbc.util.Constants.defaultKeyName;
import static com.aerospike.jdbc.util.Constants.defaultQueryLimit;
import static java.util.Collections.emptyList;

public class QueryPerformer {

    private static final Logger logger = Logger.getLogger(QueryPerformer.class.getName());

    protected final IAerospikeClient client;
    protected final AerospikeQuery query;
    protected final Statement statement;

    public QueryPerformer(IAerospikeClient client, AerospikeQuery query, Statement statement) {
        this.client = client;
        this.query = query;
        this.statement = statement;
    }

    /**
     * Executes queries against the actual Aerospike database.
     *
     * @return Pair of the ResultSet and the update count.
     */
    public Pair<ResultSet, Integer> executeQuery() {
        List<DataColumn> columns = AerospikeSchemaBuilder.getSchema(query.getSchemaTable(), client);
        Object keyObject = ExpressionBuilder.fetchKey(query.getWhere());
        Key key;

        switch (query.getType()) {
            case SELECT:
                RecordSet rs;
                // TODO count(*)
                if (Objects.nonNull(keyObject)) {
                    key = new Key(query.getSchema(), query.getTable(), Value.get(keyObject));
                    com.aerospike.client.Record record = client.get(null, key, getBinNames());
                    RecordSet recordSet = new RecordSet(2);
                    KeyRecord keyRecord = new KeyRecord(key, record);
                    recordSet.put(keyRecord);
                    recordSet.end();
                    logger.fine("__key. " + keyObject + ", " + keyRecord);
                    rs = recordSet;
                } else {
                    logger.info("SELECT scan");
                    ScanRecordSequenceListener listener = new ScanRecordSequenceListener();
                    ScanPolicy scanPolicy = buildScanPolicy();
                    client.scanAll(EventLoopProvider.getEventLoop(), listener, scanPolicy, query.getSchema(),
                            query.getTable(), getBinNames());
                    rs = listener.getRecordSet();
                }
                logger.info("Filtered columns: " + filterColumns(columns, getBinNames()));
                return new Pair<>(new AerospikeRecordResultSet(rs, statement, query.getSchema(),
                        query.getTable(), filterColumns(columns, getBinNames())), -1);

            case INSERT:
                String queryKey = extractInsertKey();
                String keyString = Objects.isNull(queryKey) ? UUID.randomUUID().toString() : queryKey;
                key = new Key(query.getSchema(), query.getTable(), keyString);

                Bin[] bins = getBins();
                logger.info("INSERT Bins to add: " + Arrays.toString(bins) + ", " +
                        query.getColumns() + ", " + query.getValues());
                client.put(buildWritePolicy(), key, bins);

                return new Pair<>(new ListRecordSet(null, query.getSchema(), query.getTable(),
                        emptyList(), emptyList()), 1);

            case DELETE:
                if (Objects.nonNull(keyObject)) {
                    key = new Key(query.getSchema(), query.getTable(), Value.get(keyObject));
                    logger.info("DELETE: " + keyObject);
                    client.delete(buildWritePolicy(), key);

                    return new Pair<>(new ListRecordSet(null, query.getSchema(), query.getTable(),
                            emptyList(), emptyList()), 1);
                }
                break;

            default:
                throw new RuntimeException("Unsupported query type");
        }
        return null;
    }

    private List<DataColumn> filterColumns(List<DataColumn> columns, String[] selected) {
        if (Objects.isNull(selected)) return columns;
        List<String> list = Arrays.stream(selected).map(IOUtils::stripQuotes).collect(Collectors.toList());
        return columns.stream().filter(c -> list.contains(c.getName())).collect(Collectors.toList());
    }

    private String[] getBinNames() {
        List<String> columns = query.getColumns();
        if (columns.size() == 1 && columns.get(0).equals("*")) {
            return null;
        }
        return columns.toArray(new String[0]);
    }

    private Bin[] getBins() {
        List<Bin> bins = new ArrayList<>();
        List<String> columns = query.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            bins.add(new Bin(IOUtils.stripQuotes(columns.get(i)), Value.get(query.getValues().get(i))));
        }
        return bins.toArray(new Bin[0]);
    }

    private String extractInsertKey() {
        List<String> columns = query.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(defaultKeyName)) {
                String key = query.getValues().get(i);
                columns.remove(i);
                query.getValues().remove(i);
                return key;
            }
        }
        return null;
    }

    private ScanPolicy buildScanPolicy() {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.maxRecords = Objects.isNull(query.getLimit()) ? defaultQueryLimit : query.getLimit();
        Exp expression = ExpressionBuilder.buildExp(query.getWhere());
        scanPolicy.filterExp = Objects.isNull(expression) ? null : Exp.build(expression);
        return scanPolicy;
    }

    private WritePolicy buildWritePolicy() {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        return writePolicy;
    }

}
