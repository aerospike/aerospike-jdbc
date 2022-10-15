package com.aerospike.jdbc.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.jdbc.util.URLParser;

public class RecordSetBatchSequenceListener implements BatchSequenceListener {

    private final RecordSet recordSet;

    public RecordSetBatchSequenceListener() {
        recordSet = new RecordSet(
                URLParser.getDriverPolicy().getRecordSetQueueCapacity(),
                URLParser.getDriverPolicy().getRecordSetTimeoutMs()
        );
    }

    @Override
    public void onRecord(BatchRead batchRead) {
        if (batchRead != null && batchRead.record != null) {
            recordSet.put(new KeyRecord(batchRead.key, batchRead.record));
        }
    }

    @Override
    public void onSuccess() {
        recordSet.close();
    }

    @Override
    public void onFailure(AerospikeException e) {
        recordSet.abort();
    }

    public RecordSet getRecordSet() {
        return recordSet;
    }
}
