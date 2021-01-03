package com.aerospike.jdbc.query;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.model.AerospikeQuery;

import java.util.Objects;

import static com.aerospike.jdbc.util.Constants.defaultQueryLimit;

public final class PolicyBuilder {

    private PolicyBuilder() {
    }

    public static ScanPolicy buildScanPolicy(AerospikeQuery query) {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.maxRecords = Objects.isNull(query.getLimit()) ? defaultQueryLimit : query.getLimit();
        Exp expression = ExpressionBuilder.buildExp(query.getWhere());
        scanPolicy.filterExp = Objects.isNull(expression) ? null : Exp.build(expression);
        return scanPolicy;
    }

    public static WritePolicy buildWritePolicy(AerospikeQuery query) {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        return writePolicy;
    }
}
