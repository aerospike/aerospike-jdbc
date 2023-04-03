package com.aerospike.jdbc.async;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.jdbc.model.AerospikeQuery;

import java.util.Objects;
import java.util.logging.Logger;

import static com.aerospike.jdbc.util.Constants.defaultKeyName;

public class ScanQueryHandler {

    private static final Logger logger = Logger.getLogger(ScanQueryHandler.class.getName());

    private final IAerospikeClient client;
    private RecordSetRecordSequenceListener listener;

    private int currentPartition;
    private int count;
    private final ScanCallback callback = ((key, record) -> {
        listener.onRecord(key, record);
        count++;
    });

    public ScanQueryHandler(IAerospikeClient client) {
        this.client = client;
        this.listener = new RecordSetRecordSequenceListener();
    }

    public static ScanQueryHandler create(IAerospikeClient client) {
        return new ScanQueryHandler(client);
    }

    public RecordSet execute(ScanPolicy scanPolicy, AerospikeQuery query) {
        if (query.getBinNames() != null && query.getBinNames().length == 1
                && query.getBinNames()[0].equals(defaultKeyName)) {
            scanPolicy.includeBinData = false;
        }
        if (Objects.nonNull(query.getOffset())) {
            long maxRecords = scanPolicy.maxRecords;
            PartitionFilter filter = getPartitionFilter(query);
            while (isScanRequired(maxRecords)) {
                client.scanPartitions(scanPolicy, filter, query.getSchema(), query.getSetName(),
                        callback, query.getBinNames());
                scanPolicy.maxRecords = maxRecords > 0 ? maxRecords - count : maxRecords;
                filter = PartitionFilter.id(++currentPartition);
            }
            listener.onSuccess();
        } else {
            client.scanAll(EventLoopProvider.getEventLoop(), listener, scanPolicy, query.getSchema(),
                    query.getSetName(), query.getBinNames());
        }
        return listener.getRecordSet();
    }

    private PartitionFilter getPartitionFilter(AerospikeQuery query) {
        Key key = new Key(query.getSchema(), query.getSetName(), query.getOffset());
        currentPartition = Partition.getPartitionId(key.digest);
        return PartitionFilter.after(key);
    }

    private boolean isScanRequired(final long maxRecords) {
        return (maxRecords == 0 || count < maxRecords) && isValidPartition();
    }

    private boolean isValidPartition() {
        return currentPartition >= 0 && currentPartition < Node.PARTITIONS;
    }
}
