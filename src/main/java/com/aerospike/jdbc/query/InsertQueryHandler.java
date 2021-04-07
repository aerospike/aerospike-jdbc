package com.aerospike.jdbc.query;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static com.aerospike.jdbc.query.PolicyBuilder.buildWritePolicy;
import static com.aerospike.jdbc.util.Constants.defaultKeyName;

public class InsertQueryHandler extends BaseQueryHandler {

    public InsertQueryHandler(IAerospikeClient client, Statement statement) {
        super(client, statement);
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        Value queryKey = extractInsertKey(query);
        if (Objects.isNull(queryKey)) {
            queryKey = generateRandomKey();
        }
        Key key = new Key(query.getSchema(), query.getTable(), queryKey);

        Bin[] bins = getBins(query);
        client.put(buildWritePolicy(query), key, bins);

        return new Pair<>(emptyRecordSet(query), 1);
    }

    private Value extractInsertKey(AerospikeQuery query) {
        List<String> columns = query.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(defaultKeyName)) {
                String key = query.getValues().get(i);
                columns.remove(i);
                query.getValues().remove(i);
                return getBinValue(key);
            }
        }
        return null;
    }

    private Value generateRandomKey() {
        return Value.get(UUID.randomUUID().toString());
    }
}
