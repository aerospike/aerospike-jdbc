package com.aerospike.jdbc.query;

import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;

import java.sql.ResultSet;

public interface QueryHandler {

    Pair<ResultSet, Integer> execute(AerospikeQuery query);
}
