package com.aerospike.jdbc.query;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.jdbc.AerospikeConnection;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.DriverConfiguration;
import com.aerospike.jdbc.sql.ListRecordSet;
import com.aerospike.jdbc.util.AerospikeVersion;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static java.util.Collections.emptyList;

public abstract class BaseQueryHandler implements QueryHandler {

    protected final IAerospikeClient client;
    protected final Statement statement;
    protected final PolicyBuilder policyBuilder;
    protected final DriverConfiguration config;
    protected final AerospikeVersion aerospikeVersion;

    protected BaseQueryHandler(IAerospikeClient client, Statement statement) {
        this.client = client;
        this.statement = statement;
        policyBuilder = new PolicyBuilder(client);
        config = getConfiguration();
        aerospikeVersion = getAerospikeVersion();
    }

    protected Bin[] getBins(AerospikeQuery query) {
        List<String> columns = query.getColumns();
        Bin[] bins = new Bin[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            bins[i] = new Bin(columns.get(i), Value.get(query.getValues().get(i)));
        }
        return bins;
    }

    protected ListRecordSet emptyRecordSet(AerospikeQuery query) {
        return new ListRecordSet(statement, query.getSchema(), query.getTable(),
                emptyList(), emptyList());
    }

    private DriverConfiguration getConfiguration() {
        try {
            return ((AerospikeConnection) statement.getConnection()).getConfiguration();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to get DriverConfiguration", e);
        }
    }

    private AerospikeVersion getAerospikeVersion() {
        try {
            return ((AerospikeConnection) statement.getConnection()).getAerospikeVersion();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to get AerospikeVersion", e);
        }
    }
}
