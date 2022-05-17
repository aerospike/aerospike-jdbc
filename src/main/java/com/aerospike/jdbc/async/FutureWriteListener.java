package com.aerospike.jdbc.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.WriteListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class FutureWriteListener implements WriteListener {

    private final CompletableFuture<Integer> totalFuture = new CompletableFuture<>();
    private final AtomicInteger totalRecordsCount = new AtomicInteger();
    private final AtomicInteger successRecordsCount = new AtomicInteger();
    private final int totalRecords;

    public FutureWriteListener(int totalRecords) {
        this.totalRecords = totalRecords;
    }

    @Override
    public void onSuccess(Key key) {
        successRecordsCount.incrementAndGet();
        tryCompleteFuture();
    }

    @Override
    public void onFailure(AerospikeException e) {
        tryCompleteFuture();
    }

    private void tryCompleteFuture() {
        if (totalRecordsCount.incrementAndGet() == totalRecords) {
            totalFuture.complete(successRecordsCount.get());
        }
    }

    public Future<Integer> getTotal() {
        return totalFuture;
    }
}
