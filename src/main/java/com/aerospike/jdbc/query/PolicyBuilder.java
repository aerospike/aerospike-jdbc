package com.aerospike.jdbc.query;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.BatchReadPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.util.URLParser;

import java.util.Objects;

public final class PolicyBuilder {

    private PolicyBuilder() {
    }

    public static ScanPolicy buildScanPolicy(AerospikeQuery query) {
        ScanPolicy scanPolicy = new ScanPolicy(URLParser.getScanPolicy());
        scanPolicy.maxRecords = Objects.isNull(query.getLimit()) ? 0 : query.getLimit();
        scanPolicy.filterExp = Objects.isNull(query.getPredicate())
                ? null : Exp.build(query.getPredicate().toFilterExpression());
        scanPolicy.sendKey = true;
        return scanPolicy;
    }

    public static QueryPolicy buildQueryPolicy(AerospikeQuery query) {
        QueryPolicy queryPolicy = new QueryPolicy(URLParser.getQueryPolicy());
        queryPolicy.filterExp = Objects.isNull(query.getPredicate())
                ? null : Exp.build(query.getPredicate().toFilterExpression());
        return queryPolicy;
    }

    public static ScanPolicy buildScanNoBinDataPolicy(AerospikeQuery query) {
        ScanPolicy scanPolicy = buildScanPolicy(query);
        scanPolicy.includeBinData = false;
        return scanPolicy;
    }

    public static WritePolicy buildWritePolicy(AerospikeQuery query) {
        WritePolicy writePolicy = new WritePolicy(URLParser.getWritePolicy());
        writePolicy.filterExp = Objects.isNull(query.getPredicate())
                ? null : Exp.build(query.getPredicate().toFilterExpression());
        writePolicy.sendKey = true;
        return writePolicy;
    }

    public static BatchReadPolicy buildBatchReadPolicy(AerospikeQuery query) {
        BatchReadPolicy policy = new BatchReadPolicy();
        policy.filterExp = Objects.isNull(query.getPredicate())
                ? null : Exp.build(query.getPredicate().toFilterExpression());
        return policy;
    }

    public static WritePolicy buildCreateOnlyPolicy() {
        WritePolicy writePolicy = new WritePolicy(URLParser.getWritePolicy());
        writePolicy.sendKey = true;
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        return writePolicy;
    }

    public static WritePolicy buildUpdateOnlyPolicy() {
        WritePolicy writePolicy = new WritePolicy(URLParser.getWritePolicy());
        writePolicy.sendKey = true;
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        return writePolicy;
    }
}
