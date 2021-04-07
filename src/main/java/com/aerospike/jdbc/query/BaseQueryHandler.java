package com.aerospike.jdbc.query;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.sql.ListRecordSet;
import com.aerospike.jdbc.util.IOUtils;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public abstract class BaseQueryHandler implements QueryHandler {

    protected final IAerospikeClient client;
    protected final Statement statement;

    public BaseQueryHandler(IAerospikeClient client, Statement statement) {
        this.client = client;
        this.statement = statement;
    }

    protected Bin[] getBins(AerospikeQuery query) {
        List<Bin> bins = new ArrayList<>();
        List<String> columns = query.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            bins.add(new Bin(IOUtils.stripQuotes(columns.get(i)), getBinValue(query.getValues().get(i))));
        }
        return bins.toArray(new Bin[0]);
    }

    protected Value getBinValue(String strValue) {
        if (ExpressionBuilder.isStringValue(strValue)) {
            return Value.get(ExpressionBuilder.stripQuotes(strValue));
        }
        try {
            return new Value.LongValue(Long.parseLong(strValue));
        } catch (NumberFormatException ignore) {
        }
        try {
            return new Value.DoubleValue(Double.parseDouble(strValue));
        } catch (NumberFormatException ignore) {
        }
        return Value.get(strValue);
    }

    protected ListRecordSet emptyRecordSet(AerospikeQuery query) {
        return new ListRecordSet(statement, query.getSchema(), query.getTable(),
                emptyList(), emptyList());
    }
}
