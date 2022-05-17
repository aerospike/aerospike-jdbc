package com.aerospike.jdbc.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.query.KeyRecord;

public class RecordSetBatchSequenceListener implements BatchSequenceListener {

    private static final int defaultCapacity = 8192;
    private final RecordSet recordSet;

    public RecordSetBatchSequenceListener() {
        recordSet = new RecordSet(defaultCapacity);
    }

    @Override
    public void onRecord(BatchRead batchRead) {
        if (batchRead != null && batchRead.record != null) {
            recordSet.put(new KeyRecord(batchRead.key, batchRead.record));
        }
    }

    @Override
    public void onSuccess() {
        recordSet.put(RecordSet.END);
    }

    @Override
    public void onFailure(AerospikeException e) {
        recordSet.close();
    }

    public RecordSet getRecordSet() {
        return recordSet;
    }
}
