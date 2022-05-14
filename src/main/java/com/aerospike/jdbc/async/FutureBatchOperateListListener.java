package com.aerospike.jdbc.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.listener.BatchOperateListListener;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class FutureBatchOperateListListener implements BatchOperateListListener {

    private final CompletableFuture<Integer> totalFuture = new CompletableFuture<>();

    @Override
    public void onSuccess(List<BatchRecord> list, boolean b) {
        totalFuture.complete((int) list.stream().filter(Objects::nonNull).count());
    }

    @Override
    public void onFailure(AerospikeException e) {
        totalFuture.completeExceptionally(e);
    }

    public Future<Integer> getTotal() {
        return totalFuture;
    }
}
