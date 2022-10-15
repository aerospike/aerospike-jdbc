package com.aerospike.jdbc.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.jdbc.util.URLParser;

public class RecordSetRecordSequenceListener implements RecordSequenceListener {

    private final RecordSet recordSet;

    public RecordSetRecordSequenceListener() {
        recordSet = new RecordSet(
                URLParser.getDriverPolicy().getRecordSetQueueCapacity(),
                URLParser.getDriverPolicy().getRecordSetTimeoutMs()
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
        recordSet.abort();
    }

    public RecordSet getRecordSet() {
        return recordSet;
    }
}
