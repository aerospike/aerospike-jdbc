package com.aerospike.jdbc.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.jdbc.model.DriverPolicy;

import java.util.logging.Level;
import java.util.logging.Logger;

public class RecordSetRecordSequenceListener implements RecordSequenceListener {

    private static final Logger logger = Logger.getLogger(RecordSetRecordSequenceListener.class.getName());

    private final RecordSet recordSet;

    public RecordSetRecordSequenceListener(DriverPolicy driverPolicy) {
        recordSet = new RecordSet(
                driverPolicy.getRecordSetQueueCapacity(),
                driverPolicy.getRecordSetTimeoutMs()
        );
    }

    @Override
    public void onRecord(Key key, Record rec) throws AerospikeException {
        recordSet.put(new KeyRecord(key, rec));
    }

    @Override
    public void onSuccess() {
        recordSet.close();
    }

    @Override
    public void onFailure(AerospikeException exception) {
        if (exception.getResultCode() == ResultCode.QUERY_TERMINATED) {
            logger.warning(exception::getMessage);
        } else {
            logger.log(Level.SEVERE, "Aerospike listener failure", exception);
        }
        recordSet.abort();
    }

    public RecordSet getRecordSet() {
        return recordSet;
    }
}
