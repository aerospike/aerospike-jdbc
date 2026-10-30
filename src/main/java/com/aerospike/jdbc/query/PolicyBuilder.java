package com.aerospike.jdbc.query;

import com.aerospike.client.BatchDelete;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.BatchPolicy;
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
        scanPolicy.filterExp = query.toFilterExpression(true);
        return scanPolicy;
    }

    public QueryPolicy buildQueryPolicy(AerospikeQuery query) {
        QueryPolicy queryPolicy = new QueryPolicy(client.getQueryPolicyDefault());
        queryPolicy.filterExp = query.toFilterExpression(true);
        return queryPolicy;
    }

    public ScanPolicy buildScanNoBinDataPolicy(AerospikeQuery query) {
        ScanPolicy scanPolicy = buildScanPolicy(query);
        scanPolicy.includeBinData = false;
        return scanPolicy;
    }

    public WritePolicy buildWritePolicy(AerospikeQuery query) {
        WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
        writePolicy.filterExp = query.toFilterExpression(true);
        writePolicy.txn = query.getTxn();
        return writePolicy;
    }

    public WritePolicy buildDeleteWritePolicy(AerospikeQuery query) {
        WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
        writePolicy.sendKey = false;
        writePolicy.txn = query.getTxn();
        return writePolicy;
    }

    public BatchPolicy buildBatchPolicyDefault(AerospikeQuery query) {
        BatchPolicy batchPolicy = new BatchPolicy(client.getBatchPolicyDefault());
        batchPolicy.txn = query.getTxn();
        return batchPolicy;
    }

    public BatchReadPolicy buildBatchReadPolicy(AerospikeQuery query) {
        BatchReadPolicy batchReadPolicy = new BatchReadPolicy();
        batchReadPolicy.filterExp = query.toFilterExpression(false);
        return batchReadPolicy;
    }

    public BatchWritePolicy buildBatchCreateOnlyPolicy() {
        return buildBatchWritePolicy(RecordExistsAction.CREATE_ONLY);
    }

    public BatchWritePolicy buildBatchUpdateOnlyPolicy() {
        return buildBatchWritePolicy(RecordExistsAction.UPDATE_ONLY);
    }

    public BatchDeletePolicy buildBatchDeletePolicy(AerospikeQuery query) {
        BatchDeletePolicy batchDeletePolicy = new BatchDeletePolicy();
        batchDeletePolicy.filterExp = query.toFilterExpression(true);
        batchDeletePolicy.sendKey = client.getBatchPolicyDefault().sendKey;
        batchDeletePolicy.durableDelete = client.getWritePolicyDefault().durableDelete;
        return batchDeletePolicy;
    }

    /**
     * Batch write policies are not populated from the connection properties,
     * so the relevant settings are taken from the configured write policy.
     */
    private BatchWritePolicy buildBatchWritePolicy(RecordExistsAction recordExistsAction) {
        WritePolicy writePolicyDefault = client.getWritePolicyDefault();
        BatchWritePolicy batchWritePolicy = new BatchWritePolicy();
        batchWritePolicy.recordExistsAction = recordExistsAction;
        batchWritePolicy.sendKey = client.getBatchPolicyDefault().sendKey;
        batchWritePolicy.expiration = writePolicyDefault.expiration;
        batchWritePolicy.durableDelete = writePolicyDefault.durableDelete;
        return batchWritePolicy;
    }

    public WritePolicy buildWritePolicy(BatchRecord batchRecord, AerospikeQuery query) {
        WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
        if (batchRecord instanceof BatchWrite) {
            BatchWritePolicy batchWritePolicy = ((BatchWrite) batchRecord).policy;
            writePolicy.recordExistsAction = batchWritePolicy.recordExistsAction;
            writePolicy.sendKey = batchWritePolicy.sendKey;
            writePolicy.expiration = batchWritePolicy.expiration;
            writePolicy.durableDelete = batchWritePolicy.durableDelete;
            writePolicy.filterExp = batchWritePolicy.filterExp;
        } else if (batchRecord instanceof BatchDelete) {
            BatchDeletePolicy batchDeletePolicy = ((BatchDelete) batchRecord).policy;
            writePolicy.sendKey = batchDeletePolicy.sendKey;
            writePolicy.durableDelete = batchDeletePolicy.durableDelete;
            writePolicy.filterExp = batchDeletePolicy.filterExp;
        }
        writePolicy.txn = query.getTxn();
        return writePolicy;
    }

    public WritePolicy buildCreateOnlyPolicy(AerospikeQuery query) {
        WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        writePolicy.txn = query.getTxn();
        return writePolicy;
    }

    public WritePolicy buildUpdateOnlyPolicy(AerospikeQuery query) {
        WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
        writePolicy.txn = query.getTxn();
        return writePolicy;
    }
}
