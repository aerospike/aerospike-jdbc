package com.aerospike.jdbc.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public final class RecordSet
        implements Iterable<KeyRecord>, Closeable {

    private static final Logger logger = Logger.getLogger(RecordSet.class.getName());

    private static final KeyRecord END = new KeyRecord(null, null);
    private static final KeyRecord FAILURE = new KeyRecord(null, null);

    private final BlockingQueue<KeyRecord> queue;
    private final int timeoutMs;
    private KeyRecord keyRecord;
    private volatile boolean valid = true;
    private volatile boolean interrupted;

    public RecordSet(int capacity, int timeoutMs) {
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.timeoutMs = timeoutMs;
    }

    public boolean next()
            throws AerospikeException {
        if (valid) {
            try {
                keyRecord = queue.take();
            } catch (InterruptedException e) {
                logger.info(() -> "InterruptedException in next");
                interrupt();
            }
            if (keyRecord == FAILURE) {
                logger.info(() -> String.format("timeoutMs: %d", timeoutMs));
                throw new AerospikeException("Aerospike asynchronous command failure");
            }
            if (keyRecord == END) {
                valid = false;
            }
        }
        return valid;
    }

    private void interrupt() {
        interrupted = true;
        queue.clear();
    }

    @Override
    public void close() {
        put(END);
    }

    @Override
    @Nonnull
    public Iterator<KeyRecord> iterator() {
        return new RecordSetIterator(this);
    }

    public Key getKey() {
        return keyRecord.key;
    }

    public Record getRecord() {
        return keyRecord.record;
    }

    public boolean put(KeyRecord keyRecord) {
        if (valid) {
            try {
                if (!queue.offer(keyRecord, timeoutMs, TimeUnit.MILLISECONDS)) {
                    logger.fine(() -> "Timeout in put");
                    abort();
                    throw new AerospikeException.QueryTerminated();
                }
            } catch (InterruptedException e) {
                logger.info(() -> "InterruptedException in put");
                abort();
                throw new AerospikeException.QueryTerminated(e);
            }
            if (interrupted) {
                logger.info(() -> "Interrupted in put");
                throw new AerospikeException.QueryTerminated();
            }
        }
        return valid;
    }

    public void abort() {
        queue.clear();
        put(FAILURE);
    }

    private static class RecordSetIterator
            implements Iterator<KeyRecord>, Closeable {
        private final RecordSet recordSet;
        private boolean more;

        RecordSetIterator(RecordSet recordSet) {
            this.recordSet = recordSet;
            this.more = this.recordSet.next();
        }

        @Override
        public boolean hasNext() {
            return more;
        }

        @Override
        public KeyRecord next() {
            KeyRecord nextKeyRecord = recordSet.keyRecord;
            more = recordSet.next();
            return nextKeyRecord;
        }

        @Override
        public void close() {
            recordSet.close();
        }
    }
}
