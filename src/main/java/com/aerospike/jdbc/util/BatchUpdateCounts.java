package com.aerospike.jdbc.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.ResultCode;
import com.aerospike.jdbc.model.QueryType;

import java.sql.BatchUpdateException;
import java.sql.Statement;
import java.util.List;

/**
 * Collects the outcome of every command of a JDBC batch and converts it into update counts.
 * <p>
 * A command that fails is reported as {@link Statement#EXECUTE_FAILED}; the remaining commands are still
 * executed. If any command failed, or the batch failed as a whole, a {@link BatchUpdateException} carrying
 * an update count for every command is thrown.
 */
public final class BatchUpdateCounts {

    static final String SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION = "23000";
    static final String SQL_STATE_GENERAL_ERROR = "HY000";

    private final int[] counts;
    private final boolean skipDuplicateKeys;

    private int failedCount;
    private int inDoubtCount;
    private int firstFailedIndex = -1;
    private int firstFailedResultCode;
    private Throwable firstFailureCause;
    private AerospikeException batchFailure;

    public BatchUpdateCounts(int size, boolean skipDuplicateKeys) {
        this.counts = new int[size];
        this.skipDuplicateKeys = skipDuplicateKeys;
    }

    /**
     * Whether a record result code means that no record was affected rather than a failure.
     */
    public static boolean isNotAffected(QueryType queryType, int resultCode, boolean skipDuplicateKeys) {
        switch (queryType) {
            case INSERT:
                return skipDuplicateKeys && resultCode == ResultCode.KEY_EXISTS_ERROR;
            case UPDATE:
            case DELETE:
                return resultCode == ResultCode.KEY_NOT_FOUND_ERROR || resultCode == ResultCode.FILTERED_OUT;
            default:
                return false;
        }
    }

    /**
     * Sets the result of a command executed as the given batch records.
     */
    public void setRecordsResult(int index, QueryType queryType, List<? extends BatchRecord> records) {
        int count = 0;
        int failedResultCode = ResultCode.OK;
        boolean failed = false;
        for (BatchRecord batchRecord : records) {
            if (batchRecord.resultCode == ResultCode.OK) {
                count++;
            } else if (!isNotAffected(queryType, batchRecord.resultCode, skipDuplicateKeys)) {
                if (!failed) {
                    failed = true;
                    failedResultCode = batchRecord.resultCode;
                }
                if (batchRecord.inDoubt) {
                    inDoubtCount++;
                }
            }
        }
        if (failed) {
            setFailed(index, failedResultCode, null);
        } else {
            counts[index] = count;
        }
    }

    public void setCount(int index, int count) {
        counts[index] = count;
    }

    public void setFailed(int index, Throwable cause) {
        setFailed(index, resultCodeOf(cause), cause);
    }

    /**
     * Registers a failure that affected the batch as a whole, e.g. a timeout or a rejected request.
     */
    public void setBatchFailure(AerospikeException e) {
        if (batchFailure == null) {
            batchFailure = e;
        }
    }

    public int[] getCounts() throws BatchUpdateException {
        if (failedCount == 0 && batchFailure == null) {
            return counts;
        }
        int vendorCode = batchFailure != null ? batchFailure.getResultCode() : firstFailedResultCode;
        String sqlState = vendorCode == ResultCode.KEY_EXISTS_ERROR
                ? SQL_STATE_INTEGRITY_CONSTRAINT_VIOLATION
                : SQL_STATE_GENERAL_ERROR;
        Throwable cause = batchFailure != null ? batchFailure : firstFailureCause;
        throw new BatchUpdateException(buildMessage(cause), sqlState, vendorCode, counts, cause);
    }

    private void setFailed(int index, int resultCode, Throwable cause) {
        counts[index] = Statement.EXECUTE_FAILED;
        failedCount++;
        if (firstFailedIndex < 0) {
            firstFailedIndex = index;
            firstFailedResultCode = resultCode;
            firstFailureCause = cause;
        }
    }

    private String buildMessage(Throwable cause) {
        StringBuilder sb = new StringBuilder()
                .append("Batch execution failed: ")
                .append(failedCount).append(" of ").append(counts.length).append(" commands failed");
        if (firstFailedIndex >= 0) {
            sb.append("; first failure at index ").append(firstFailedIndex).append(": ")
                    .append(ResultCode.getResultString(firstFailedResultCode))
                    .append(" (result code ").append(firstFailedResultCode).append(')');
        }
        if (inDoubtCount > 0) {
            sb.append("; ").append(inDoubtCount).append(" record writes are in doubt and may have been applied");
        }
        if (cause != null && cause.getMessage() != null) {
            sb.append("; cause: ").append(cause.getMessage());
        }
        return sb.toString();
    }

    private static int resultCodeOf(Throwable cause) {
        for (Throwable t = cause; t != null; t = t.getCause()) {
            if (t instanceof AerospikeException) {
                return ((AerospikeException) t).getResultCode();
            }
        }
        return ResultCode.CLIENT_ERROR;
    }
}
