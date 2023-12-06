package com.aerospike.jdbc.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.jdbc.model.DriverPolicy;

import java.util.logging.Logger;

public class RecordSetBatchSequenceListener implements BatchSequenceListener {

    private static final Logger logger = Logger.getLogger(RecordSetBatchSequenceListener.class.getName());

    private final RecordSet recordSet;

    public RecordSetBatchSequenceListener(DriverPolicy driverPolicy) {
        recordSet = new RecordSet(
                driverPolicy.getRecordSetQueueCapacity(),
                driverPolicy.getRecordSetTimeoutMs()
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
        logger.warning(e::getMessage);
        recordSet.abort();
    }

    public RecordSet getRecordSet() {
        return recordSet;
    }
}
