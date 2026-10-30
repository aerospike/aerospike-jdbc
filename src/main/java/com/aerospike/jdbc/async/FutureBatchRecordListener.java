package com.aerospike.jdbc.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.RecordListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stores the results of individually executed single-record commands in their {@link BatchRecord}s,
 * emulating a batch on servers that do not support batch writes.
 */
public class FutureBatchRecordListener {

    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private final AtomicInteger remaining;

    public FutureBatchRecordListener(int totalRecords) {
        remaining = new AtomicInteger(totalRecords);
        if (totalRecords == 0) {
            completion.complete(null);
        }
    }

    public RecordListener writeListener(BatchRecord batchRecord) {
        return new RecordListener() {
            @Override
            public void onSuccess(Key key, Record rec) {
                setResult(batchRecord, ResultCode.OK, false);
            }

            @Override
            public void onFailure(AerospikeException ae) {
                FutureBatchRecordListener.this.onFailure(batchRecord, ae);
            }
        };
    }

    public DeleteListener deleteListener(BatchRecord batchRecord) {
        return new DeleteListener() {
            @Override
            public void onSuccess(Key key, boolean existed) {
                setResult(batchRecord, existed ? ResultCode.OK : ResultCode.KEY_NOT_FOUND_ERROR, false);
            }

            @Override
            public void onFailure(AerospikeException ae) {
                FutureBatchRecordListener.this.onFailure(batchRecord, ae);
            }
        };
    }

    public void onFailure(BatchRecord batchRecord, AerospikeException ae) {
        setResult(batchRecord, ae.getResultCode(), ae.getInDoubt());
    }

    public Future<Void> getCompletion() {
        return completion;
    }

    private void setResult(BatchRecord batchRecord, int resultCode, boolean inDoubt) {
        batchRecord.setError(resultCode, inDoubt);
        if (remaining.decrementAndGet() == 0) {
            completion.complete(null);
        }
    }
}
