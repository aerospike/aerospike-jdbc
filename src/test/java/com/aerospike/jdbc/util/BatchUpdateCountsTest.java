package com.aerospike.jdbc.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchDelete;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.jdbc.async.FutureBatchRecordListener;
import com.aerospike.jdbc.model.DriverPolicy;
import com.aerospike.jdbc.model.QueryType;
import org.testng.annotations.Test;

import java.sql.BatchUpdateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static java.sql.Statement.EXECUTE_FAILED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class BatchUpdateCountsTest {

    private static BatchRecord write(String key, int resultCode) {
        return write(key, resultCode, false);
    }

    private static BatchRecord write(String key, int resultCode, boolean inDoubt) {
        BatchRecord batchRecord = new BatchWrite(new Key("test", "jdbc", key), new Operation[0]);
        batchRecord.setError(resultCode, inDoubt);
        return batchRecord;
    }

    private static BatchRecord delete(String key, int resultCode) {
        BatchRecord batchRecord = new BatchDelete(new Key("test", "jdbc", key));
        batchRecord.setError(resultCode, false);
        return batchRecord;
    }

    private static BatchUpdateCounts inserts(boolean skipDuplicateKeys, BatchRecord... records) {
        BatchUpdateCounts updateCounts = new BatchUpdateCounts(records.length, skipDuplicateKeys);
        for (int i = 0; i < records.length; i++) {
            updateCounts.setRecordsResult(i, QueryType.INSERT, Collections.singletonList(records[i]));
        }
        return updateCounts;
    }

    @Test
    public void testAllSucceeded() throws BatchUpdateException {
        BatchUpdateCounts updateCounts = inserts(false,
                write("k1", ResultCode.OK), write("k2", ResultCode.OK), write("k3", ResultCode.OK));
        assertEquals(updateCounts.getCounts(), new int[]{1, 1, 1});
    }

    @Test
    public void testDuplicateKeyFails() {
        BatchUpdateCounts updateCounts = inserts(false,
                write("k1", ResultCode.OK), write("k2", ResultCode.KEY_EXISTS_ERROR), write("k3", ResultCode.OK));

        BatchUpdateException e = expectThrows(BatchUpdateException.class, updateCounts::getCounts);
        assertEquals(e.getUpdateCounts(), new int[]{1, EXECUTE_FAILED, 1});
        assertEquals(e.getErrorCode(), ResultCode.KEY_EXISTS_ERROR);
        assertEquals(e.getSQLState(), BatchUpdateCounts.SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION);
        assertNull(e.getCause());
        assertTrue(e.getMessage().contains("1 of 3 commands failed"), e.getMessage());
        assertTrue(e.getMessage().contains("index 1"), e.getMessage());
    }

    @Test
    public void testDuplicateKeySkipped() throws BatchUpdateException {
        BatchUpdateCounts updateCounts = inserts(true,
                write("k1", ResultCode.OK), write("k2", ResultCode.KEY_EXISTS_ERROR), write("k3", ResultCode.OK));
        assertEquals(updateCounts.getCounts(), new int[]{1, 0, 1});
    }

    @Test
    public void testDuplicateKeySkippedWithOtherFailure() {
        BatchUpdateCounts updateCounts = inserts(true,
                write("k1", ResultCode.OK), write("k2", ResultCode.KEY_EXISTS_ERROR),
                write("k3", ResultCode.RECORD_TOO_BIG));

        BatchUpdateException e = expectThrows(BatchUpdateException.class, updateCounts::getCounts);
        assertEquals(e.getUpdateCounts(), new int[]{1, 0, EXECUTE_FAILED});
        assertEquals(e.getErrorCode(), ResultCode.RECORD_TOO_BIG);
        assertEquals(e.getSQLState(), BatchUpdateCounts.SQL_STATE_GENERAL_ERROR);
    }

    @Test
    public void testUpdateAndDeleteNotFoundAreNotFailures() throws BatchUpdateException {
        BatchUpdateCounts updateCounts = new BatchUpdateCounts(4, false);
        updateCounts.setRecordsResult(0, QueryType.UPDATE, Collections.singletonList(
                write("k1", ResultCode.OK)));
        updateCounts.setRecordsResult(1, QueryType.UPDATE, Collections.singletonList(
                write("k2", ResultCode.KEY_NOT_FOUND_ERROR)));
        updateCounts.setRecordsResult(2, QueryType.DELETE, Arrays.asList(
                delete("k3", ResultCode.OK), delete("k4", ResultCode.KEY_NOT_FOUND_ERROR),
                delete("k5", ResultCode.OK)));
        updateCounts.setRecordsResult(3, QueryType.DELETE, Collections.singletonList(
                delete("k6", ResultCode.FILTERED_OUT)));
        assertEquals(updateCounts.getCounts(), new int[]{1, 0, 2, 0});
    }

    @Test
    public void testInsertNotFoundIsFailure() {
        BatchUpdateCounts updateCounts = inserts(true, write("k1", ResultCode.KEY_NOT_FOUND_ERROR));
        BatchUpdateException e = expectThrows(BatchUpdateException.class, updateCounts::getCounts);
        assertEquals(e.getUpdateCounts(), new int[]{EXECUTE_FAILED});
    }

    @Test
    public void testMultiRecordCommandWithFailedRecord() {
        BatchUpdateCounts updateCounts = new BatchUpdateCounts(1, false);
        updateCounts.setRecordsResult(0, QueryType.INSERT, Arrays.asList(
                write("k1", ResultCode.OK), write("k2", ResultCode.KEY_EXISTS_ERROR)));
        BatchUpdateException e = expectThrows(BatchUpdateException.class, updateCounts::getCounts);
        assertEquals(e.getUpdateCounts(), new int[]{EXECUTE_FAILED});
    }

    @Test
    public void testBatchTimeout() {
        AerospikeException timeout = new AerospikeException(ResultCode.TIMEOUT, "Client timeout");
        BatchUpdateCounts updateCounts = inserts(false,
                write("k1", ResultCode.OK),
                write("k2", ResultCode.NO_RESPONSE, true),
                write("k3", ResultCode.NO_RESPONSE, true));
        updateCounts.setBatchFailure(timeout);

        BatchUpdateException e = expectThrows(BatchUpdateException.class, updateCounts::getCounts);
        assertEquals(e.getUpdateCounts(), new int[]{1, EXECUTE_FAILED, EXECUTE_FAILED});
        assertEquals(e.getErrorCode(), ResultCode.TIMEOUT);
        assertEquals(e.getSQLState(), BatchUpdateCounts.SQL_STATE_GENERAL_ERROR);
        assertSame(e.getCause(), timeout);
        assertTrue(e.getMessage().contains("2 record writes are in doubt"), e.getMessage());
    }

    @Test
    public void testBatchRejected() {
        AerospikeException rejected = new AerospikeException(ResultCode.INVALID_NAMESPACE, "Namespace not found");
        BatchUpdateCounts updateCounts = inserts(false,
                write("k1", ResultCode.INVALID_NAMESPACE), write("k2", ResultCode.INVALID_NAMESPACE));
        updateCounts.setBatchFailure(rejected);

        BatchUpdateException e = expectThrows(BatchUpdateException.class, updateCounts::getCounts);
        assertEquals(e.getUpdateCounts(), new int[]{EXECUTE_FAILED, EXECUTE_FAILED});
        assertEquals(e.getErrorCode(), ResultCode.INVALID_NAMESPACE);
        assertSame(e.getCause(), rejected);
    }

    @Test
    public void testBatchFailureWithoutFailedRecords() {
        BatchUpdateCounts updateCounts = inserts(false, write("k1", ResultCode.OK));
        updateCounts.setBatchFailure(new AerospikeException(ResultCode.SERVER_ERROR, "Server error"));

        BatchUpdateException e = expectThrows(BatchUpdateException.class, updateCounts::getCounts);
        assertEquals(e.getUpdateCounts(), new int[]{1});
        assertEquals(e.getErrorCode(), ResultCode.SERVER_ERROR);
    }

    @Test
    public void testIndividualCommandCountsAndFailures() {
        BatchUpdateCounts updateCounts = new BatchUpdateCounts(3, false);
        updateCounts.setCount(0, 5);
        IllegalStateException cause = new IllegalStateException(
                new AerospikeException(ResultCode.INDEX_NOTFOUND, "Index not found"));
        updateCounts.setFailed(1, cause);
        updateCounts.setFailed(2, new IllegalArgumentException("Column not found"));

        BatchUpdateException e = expectThrows(BatchUpdateException.class, updateCounts::getCounts);
        assertEquals(e.getUpdateCounts(), new int[]{5, EXECUTE_FAILED, EXECUTE_FAILED});
        assertEquals(e.getErrorCode(), ResultCode.INDEX_NOTFOUND);
        assertSame(e.getCause(), cause);
    }

    @Test
    public void testFutureBatchRecordListener() throws Exception {
        List<BatchRecord> records = Arrays.asList(
                new BatchWrite(new Key("test", "jdbc", "k1"), new Operation[0]),
                new BatchWrite(new Key("test", "jdbc", "k2"), new Operation[0]),
                new BatchDelete(new Key("test", "jdbc", "k3")),
                new BatchDelete(new Key("test", "jdbc", "k4")));
        FutureBatchRecordListener listener = new FutureBatchRecordListener(records.size());

        listener.writeListener(records.get(0)).onSuccess(records.get(0).key, null);
        listener.writeListener(records.get(1)).onFailure(
                new AerospikeException(ResultCode.KEY_EXISTS_ERROR, "Key exists"));
        listener.deleteListener(records.get(2)).onSuccess(records.get(2).key, true);
        assertFalse(listener.getCompletion().isDone());
        listener.deleteListener(records.get(3)).onSuccess(records.get(3).key, false);
        assertTrue(listener.getCompletion().isDone());
        listener.getCompletion().get();

        assertEquals(records.get(0).resultCode, ResultCode.OK);
        assertEquals(records.get(1).resultCode, ResultCode.KEY_EXISTS_ERROR);
        assertEquals(records.get(2).resultCode, ResultCode.OK);
        assertEquals(records.get(3).resultCode, ResultCode.KEY_NOT_FOUND_ERROR);
    }

    @Test
    public void testFutureBatchRecordListenerEmpty() {
        assertTrue(new FutureBatchRecordListener(0).getCompletion().isDone());
    }

    @Test
    public void testBatchSkipDuplicateKeysProperty() {
        assertFalse(new DriverPolicy(new Properties()).getBatchSkipDuplicateKeys());

        Properties properties = new Properties();
        properties.setProperty("batchSkipDuplicateKeys", "true");
        assertTrue(new DriverPolicy(properties).getBatchSkipDuplicateKeys());
    }
}
