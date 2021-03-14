package com.aerospike.jdbc.scan;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.query.KeyRecord;

import static com.aerospike.jdbc.util.Constants.defaultQueryLimit;

public class PartitionScanCallback
        implements ScanCallback {

    private static final int defaultCapacity = defaultQueryLimit + 1;
    private final RecordSet recordSet;

    public PartitionScanCallback() {
        recordSet = new RecordSet(defaultCapacity);
    }

    @Override
    public void scanCallback(Key key, Record record)
            throws AerospikeException {
        recordSet.put(new KeyRecord(key, record));
    }

    public RecordSet getRecordSet() {
        return recordSet;
    }
}
