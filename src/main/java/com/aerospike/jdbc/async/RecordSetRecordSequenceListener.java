package com.aerospike.jdbc.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.query.KeyRecord;

public class RecordSetRecordSequenceListener
        implements RecordSequenceListener {

    private static final int defaultCapacity = 8192;
    private final RecordSet recordSet;

    public RecordSetRecordSequenceListener() {
        recordSet = new RecordSet(defaultCapacity);
    }

    @Override
    public void onRecord(Key key, Record record)
            throws AerospikeException {
        recordSet.put(new KeyRecord(key, record));
    }

    @Override
    public void onSuccess() {
        recordSet.put(RecordSet.END);
    }

    @Override
    public void onFailure(AerospikeException exception) {
        recordSet.close();
    }

    public RecordSet getRecordSet() {
        return recordSet;
    }
}
