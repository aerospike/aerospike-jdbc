package com.aerospike.jdbc.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.BatchReadPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.model.AerospikeQuery;

import java.util.Objects;

public class PolicyBuilder {

    protected final IAerospikeClient client;

    public PolicyBuilder(IAerospikeClient client) {
        this.client = client;
    }

    public ScanPolicy buildScanPolicy(AerospikeQuery query) {
        ScanPolicy scanPolicy = new ScanPolicy(client.getScanPolicyDefault());
        scanPolicy.maxRecords = Objects.isNull(query.getLimit()) ? 0 : query.getLimit();
        scanPolicy.filterExp = Objects.isNull(query.getPredicate())
                ? null : Exp.build(query.getPredicate().toFilterExpression());
        return scanPolicy;
    }

    public QueryPolicy buildQueryPolicy(AerospikeQuery query) {
        QueryPolicy queryPolicy = new QueryPolicy(client.getQueryPolicyDefault());
        queryPolicy.filterExp = Objects.isNull(query.getPredicate())
                ? null : Exp.build(query.getPredicate().toFilterExpression());
        return queryPolicy;
    }

    public ScanPolicy buildScanNoBinDataPolicy(AerospikeQuery query) {
        ScanPolicy scanPolicy = buildScanPolicy(query);
        scanPolicy.includeBinData = false;
        return scanPolicy;
    }

    public WritePolicy buildWritePolicy(AerospikeQuery query) {
        WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
        writePolicy.filterExp = Objects.isNull(query.getPredicate())
                ? null : Exp.build(query.getPredicate().toFilterExpression());
        return writePolicy;
    }

    public BatchReadPolicy buildBatchReadPolicy(AerospikeQuery query) {
        BatchReadPolicy batchReadPolicy = new BatchReadPolicy();
        batchReadPolicy.filterExp = Objects.isNull(query.getPredicate())
                ? null : Exp.build(query.getPredicate().toFilterExpression());
        return batchReadPolicy;
    }

    public BatchWritePolicy buildBatchCreateOnlyPolicy() {
        BatchWritePolicy batchWritePolicy = new BatchWritePolicy();
        batchWritePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        batchWritePolicy.sendKey = client.getBatchPolicyDefault().sendKey;
        batchWritePolicy.expiration = client.getBatchWritePolicyDefault().expiration;
        return batchWritePolicy;
    }

    public WritePolicy buildCreateOnlyPolicy() {
        WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        return writePolicy;
    }

    public WritePolicy buildUpdateOnlyPolicy() {
        WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        return writePolicy;
    }
}
