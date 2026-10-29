package com.aerospike.jdbc.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.jdbc.AerospikeConnection;
import com.aerospike.jdbc.AerospikeDatabaseMetadata;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.DriverConfiguration;
import com.aerospike.jdbc.sql.ListRecordSet;
import com.aerospike.jdbc.util.AerospikeVersion;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.Collections.emptyList;

public abstract class BaseQueryHandler implements QueryHandler {

    private static final Logger logger = Logger.getLogger(BaseQueryHandler.class.getName());

    protected final IAerospikeClient client;
    protected final Statement statement;
    protected final PolicyBuilder policyBuilder;
    protected final DriverConfiguration config;
    protected final AerospikeVersion aerospikeVersion;
    protected final AerospikeDatabaseMetadata databaseMetadata;

    protected boolean strict;

    protected BaseQueryHandler(IAerospikeClient client, Statement statement) {
        this.client = client;
        this.statement = statement;
        policyBuilder = new PolicyBuilder(client);
        config = getConfiguration();
        aerospikeVersion = getAerospikeVersion();
        databaseMetadata = getDatabaseMetadata();
    }

    protected Bin[] getBins(AerospikeQuery query) {
        List<String> columns = query.getColumns();
        Bin[] bins = new Bin[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            bins[i] = new Bin(columns.get(i), Value.get(query.getValues().get(i)));
        }
        return bins;
    }

    protected ListRecordSet emptyRecordSet(AerospikeQuery query) {
        return new ListRecordSet(statement, query.getCatalog(), query.getTable(),
                emptyList(), emptyList());
    }

    /**
     * In strict mode, failures that would otherwise only be logged are rethrown.
     * Used by batch execution to report per-command failures.
     */
    public void setStrict(boolean strict) {
        this.strict = strict;
    }

    protected Integer getUpdateCount(Future<Integer> updateCountFuture) {
        try {
            return updateCountFuture.get();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "Future computation failure", e.getCause());
            if (strict) {
                throw e.getCause() instanceof AerospikeException
                        ? (AerospikeException) e.getCause()
                        : new IllegalStateException(e.getCause());
            }
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Thread was interrupted", e);
            if (strict) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Thread was interrupted", e);
            }
        }
        return 0;
    }

    protected void logAerospikeException(AerospikeException e) {
        logger.log(Level.SEVERE, "Aerospike operation failure", e);
    }

    /**
     * Handles a single record write failure during a scan-based UPDATE or DELETE.
     * Records that disappeared or were filtered out after the scan are not failures.
     *
     * @return the first failure to report in strict mode
     */
    protected AerospikeException onScanWriteFailure(AerospikeException e, AerospikeException firstFailure) {
        logAerospikeException(e);
        if (!strict || firstFailure != null
                || e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR
                || e.getResultCode() == ResultCode.FILTERED_OUT) {
            return firstFailure;
        }
        return e;
    }

    private DriverConfiguration getConfiguration() {
        try {
            return ((AerospikeConnection) statement.getConnection()).getConfiguration();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to get DriverConfiguration", e);
        }
    }

    private AerospikeVersion getAerospikeVersion() {
        try {
            return ((AerospikeConnection) statement.getConnection()).getAerospikeVersion();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to get AerospikeVersion", e);
        }
    }

    private AerospikeDatabaseMetadata getDatabaseMetadata() {
        try {
            return (AerospikeDatabaseMetadata) statement.getConnection().getMetaData();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to get AerospikeDatabaseMetadata", e);
        }
    }
}
