package com.aerospike.jdbc;

import com.aerospike.client.AbortStatus;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.CommitStatus;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Txn;
import com.aerospike.client.policy.Policy;
import com.aerospike.jdbc.model.DriverConfiguration;
import com.aerospike.jdbc.sql.SimpleWrapper;
import com.aerospike.jdbc.sql.type.ByteArrayBlob;
import com.aerospike.jdbc.sql.type.StringClob;
import com.aerospike.jdbc.util.AerospikeVersion;
import com.aerospike.jdbc.util.DatabaseMetadataBuilder;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;

public class AerospikeConnection implements Connection, SimpleWrapper {

    private static final Logger logger = Logger.getLogger(AerospikeConnection.class.getName());
    private static final String SAVEPOINT_NOT_SUPPORTED_MESSAGE = "Savepoint is not supported";

    private final String url;
    private final DriverConfiguration config;
    private final IAerospikeClient client;
    private final DatabaseMetadataBuilder metadataBuilder;
    private final AerospikeVersion aerospikeVersion;
    private final AtomicReference<String> catalog = new AtomicReference<>(null);

    private volatile boolean readOnly = false;
    private volatile Map<String, Class<?>> typeMap = emptyMap();
    private volatile int holdability = HOLD_CURSORS_OVER_COMMIT;
    private volatile boolean closed;
    private boolean autoCommit = true;
    private Txn txn;

    public AerospikeConnection(String url, Properties props) {
        logger.info("Init AerospikeConnection");
        this.url = url;
        config = new DriverConfiguration(props);
        client = config.parse(url);
        metadataBuilder = new DatabaseMetadataBuilder(config.getDriverPolicy());
        aerospikeVersion = new AerospikeVersion(client);
        catalog.set(config.getCatalog()); // namespace
    }

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        return sql;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (autoCommit) {
            txn = null;
        }
        this.autoCommit = autoCommit;
    }

    /**
     * Requires Aerospike Server 8.0+.
     *
     * @throws SQLException if the transaction commit fails.
     */
    @Override
    public void commit() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Connection is in auto-commit mode");
        }
        if (txn == null) {
            throw new SQLException("txn is null");
        }
        try {
            CommitStatus status = client.commit(txn);
            logger.info(() -> format("MRT %d commit status: %s", txn.getId(), status));
        } catch (AerospikeException e) {
            throw new SQLException(e);
        } finally {
            txn = null;
        }
    }

    /**
     * Requires Aerospike Server 8.0+.
     *
     * @throws SQLException if the transaction rollback fails.
     */
    @Override
    public void rollback() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Connection is in auto-commit mode");
        }
        if (txn == null) {
            throw new SQLException("txn is null");
        }
        try {
            AbortStatus status = client.abort(txn);
            logger.info(() -> format("MRT %d rollback status: %s", txn.getId(), status));
        } catch (AerospikeException e) {
            throw new SQLException(e);
        } finally {
            txn = null;
        }
    }

    @Override
    public void close() {
        logger.info(() -> "Close AerospikeConnection");
        client.close();
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        logger.fine(() -> "getMetaData request");
        return metadataBuilder.build(url, this);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return readOnly;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        this.readOnly = readOnly;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return catalog.get();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        this.catalog.set(catalog);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        if (level != TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException(format("Aerospike does not support transactions," +
                    " so the only valid value here is TRANSACTION_NONE=%d", TRANSACTION_NONE));
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        // no-op
    }

    @Override
    @SuppressWarnings("MagicConstant")
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, holdability);
    }

    @Override
    @SuppressWarnings("MagicConstant")
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, holdability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql, resultSetType, resultSetConcurrency, holdability);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        return typeMap;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        typeMap = map;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return holdability;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        if (holdability != HOLD_CURSORS_OVER_COMMIT && holdability != CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLException(format(
                    "Unsupported holdability %d. Must be either HOLD_CURSORS_OVER_COMMIT=%d or CLOSE_CURSORS_AT_COMMIT=%d",
                    holdability,
                    HOLD_CURSORS_OVER_COMMIT,
                    CLOSE_CURSORS_AT_COMMIT));
        }

        this.holdability = holdability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException(SAVEPOINT_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException(SAVEPOINT_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException(SAVEPOINT_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException(SAVEPOINT_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkClosed();
        checkTxn();
        validateResultSetParameters(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new AerospikeStatement(client, this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        checkClosed();
        checkTxn();
        validateResultSetParameters(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new AerospikePreparedStatement(client, this, sql);
    }

    private void checkTxn() {
        if (!autoCommit && txn == null) {
            txn = new Txn();
            txn.setTimeout(config.getDriverPolicy().getTxnTimeoutSeconds());
        }
    }

    private void validateResultSetParameters(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        if (resultSetType != TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("ResultSet type other than TYPE_FORWARD_ONLY is not supported");
        }
        if (resultSetConcurrency != CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Updatable ResultSet is not supported yet");
        }
        if (resultSetHoldability != HOLD_CURSORS_OVER_COMMIT && resultSetHoldability != CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLException(format("Wrong value for the resultSetHoldability (%d). Supported values are: " +
                            "HOLD_CURSORS_OVER_COMMIT=%d or CLOSE_CURSORS_AT_COMMIT=%d", resultSetHoldability,
                    HOLD_CURSORS_OVER_COMMIT, CLOSE_CURSORS_AT_COMMIT));
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall is not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        return createNClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkClosed();
        return new ByteArrayBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();
        return new StringClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML is not supported");
    }

    @Override
    public boolean isValid(int timeout) {
        return !isClosed() && client.isConnected() && Objects.nonNull(client.getClusterStats());
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            checkClosed();
        } catch (final SQLException cause) {
            Map<String, ClientInfoStatus> failedProperties = new HashMap<>();
            failedProperties.put(name, ClientInfoStatus.REASON_UNKNOWN);
            throw new SQLClientInfoException(failedProperties, cause);
        }
        logger.info(() -> format("Set client info: %s -> %s", name, value));
        config.put(name, value);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return config.getClientInfo().getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return config.getClientInfo();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            checkClosed();
        } catch (final SQLException cause) {
            Map<String, ClientInfoStatus> failedProperties = new HashMap<>();
            for (Object key : properties.keySet()) {
                failedProperties.put((String) key, ClientInfoStatus.REASON_UNKNOWN);
            }
            throw new SQLClientInfoException(failedProperties, cause);
        }
        logger.info(() -> format("Set client info: %s", properties));
        config.putAll(properties);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    @SuppressWarnings("java:S4144")
    public String getSchema() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        // no-op
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw new SQLException("executor is null");
        }
        executor.execute(this::close);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkClosed();
        if (milliseconds < 0) {
            throw new SQLException("Timeout must be a non-negative value in milliseconds");
        }

        stream(new Policy[]{
                client.getReadPolicyDefault(),
                client.getWritePolicyDefault(),
                client.getScanPolicyDefault(),
                client.getQueryPolicyDefault(),
                client.getBatchPolicyDefault()
        }).forEach(p -> {
            p.totalTimeout = milliseconds;
            p.connectTimeout = milliseconds;
        });
        client.getInfoPolicyDefault().timeout = milliseconds;
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        return client.getReadPolicyDefault().totalTimeout;
    }

    protected void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }

    public DriverConfiguration getConfiguration() {
        return config;
    }

    public AerospikeVersion getAerospikeVersion() {
        return aerospikeVersion;
    }

    public IAerospikeClient getClient() {
        return client;
    }

    public Txn getTxn() {
        return txn;
    }
}
