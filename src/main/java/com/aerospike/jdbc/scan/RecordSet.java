package com.aerospike.jdbc.scan;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public final class RecordSet
        implements Iterable<KeyRecord>, Closeable {

    public static final KeyRecord END = new KeyRecord(null, null);
    private final BlockingQueue<KeyRecord> queue;
    private KeyRecord record;
    private volatile boolean valid = true;

    public RecordSet(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    public final boolean next()
            throws AerospikeException {
        try {
            this.record = this.queue.take();
        } catch (InterruptedException e) {
            this.valid = false;
            return false;
        }

        if (this.record == END) {
            this.record = null;
            this.valid = false;
            return false;
        } else {
            return true;
        }
    }

    public final void close() {
        this.valid = false;
    }

    public Iterator<KeyRecord> iterator() {
        return new RecordSetIterator(this);
    }

    public final Key getKey() {
        return this.record.key;
    }

    public final Record getRecord() {
        return this.record.record;
    }

    public final boolean put(KeyRecord record) {
        if (!this.valid) {
            return false;
        } else {
            try {
                this.queue.put(record);
                return true;
            } catch (InterruptedException e) {
                if (this.valid) {
                    this.abort();
                }

                return false;
            }
        }
    }

    public final boolean end() {
        return put(END);
    }

    protected final void abort() {
        this.valid = false;
        this.queue.clear();

        while (true) {
            if (this.queue.offer(END) && this.queue.poll() == null) {
                break;
            }
        }
    }

    private static class RecordSetIterator
            implements Iterator<KeyRecord>, Closeable {
        private final RecordSet recordSet;
        private boolean more;

        RecordSetIterator(RecordSet recordSet) {
            this.recordSet = recordSet;
            this.more = this.recordSet.next();
        }

        public boolean hasNext() {
            return this.more;
        }

        public KeyRecord next() {
            KeyRecord kr = this.recordSet.record;
            this.more = this.recordSet.next();
            return kr;
        }

        public void remove() {
        }

        public void close() {
            this.recordSet.close();
        }
    }
}
