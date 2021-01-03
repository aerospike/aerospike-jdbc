package com.aerospike.jdbc.query;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;
import com.aerospike.jdbc.sql.ListRecordSet;
import com.aerospike.jdbc.util.IOUtils;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static com.aerospike.jdbc.query.PolicyBuilder.buildWritePolicy;
import static com.aerospike.jdbc.util.Constants.defaultKeyName;
import static java.util.Collections.emptyList;

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

        return new Pair<>(new ListRecordSet(null, query.getSchema(), query.getTable(),
                emptyList(), emptyList()), 1);
    }

    private Bin[] getBins(AerospikeQuery query) {
        List<Bin> bins = new ArrayList<>();
        List<String> columns = query.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            bins.add(new Bin(IOUtils.stripQuotes(columns.get(i)), Value.get(query.getValues().get(i))));
        }
        return bins.toArray(new Bin[0]);
    }

    private Value extractInsertKey(AerospikeQuery query) {
        List<String> columns = query.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(defaultKeyName)) {
                String key = query.getValues().get(i);
                columns.remove(i);
                query.getValues().remove(i);
                return Value.get(key);
            }
        }
        return null;
    }

    private Value generateRandomKey() {
        return Value.get(UUID.randomUUID().toString());
    }
}
