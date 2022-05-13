package com.aerospike.jdbc.query;

import com.aerospike.client.AerospikeException;
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

import static com.aerospike.jdbc.query.PolicyBuilder.buildCreateOnlyPolicy;
import static com.aerospike.jdbc.util.Constants.defaultKeyName;

public class InsertQueryHandler extends BaseQueryHandler {

    public InsertQueryHandler(IAerospikeClient client, Statement statement) {
        super(client, statement);
    }

    @Override
    public Pair<ResultSet, Integer> execute(AerospikeQuery query) {
        Value queryKey = extractInsertKey(query, 0);
        if (Objects.isNull(queryKey)) {
            queryKey = generateRandomKey();
        }
        Key key = new Key(query.getSchema(), query.getTable(), queryKey);

        Bin[] bins = getInsertBins(query, 0);
        try {
            client.put(buildCreateOnlyPolicy(query), key, bins);
        } catch (AerospikeException e) {
            return new Pair<>(emptyRecordSet(query), 0);
        }

        return new Pair<>(emptyRecordSet(query), 1);
    }

    private Value extractInsertKey(AerospikeQuery query, int n) {
        List<String> columns = query.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(defaultKeyName)) {
                Object key = ((List<Object>) query.getValues().get(n)).get(i);
                columns.remove(i);
                ((List<Object>) query.getValues().get(n)).remove(i);
                return Value.get(key);
            }
        }
        return null;
    }

    private Value generateRandomKey() {
        return Value.get(UUID.randomUUID().toString());
    }
}
