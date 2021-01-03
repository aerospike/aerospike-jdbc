package com.aerospike.jdbc.query;

import com.aerospike.client.IAerospikeClient;

import java.sql.Statement;

public abstract class BaseQueryHandler implements QueryHandler {

    protected final IAerospikeClient client;
    protected final Statement statement;

    public BaseQueryHandler(IAerospikeClient client, Statement statement) {
        this.client = client;
        this.statement = statement;
    }
}
