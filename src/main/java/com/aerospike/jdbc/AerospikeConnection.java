package com.aerospike.jdbc;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.Policy;
import com.aerospike.jdbc.sql.SimpleWrapper;
import com.aerospike.jdbc.sql.type.ByteArrayBlob;
import com.aerospike.jdbc.sql.type.StringClob;
import com.aerospike.jdbc.util.URLParser;

import java.sql.*;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.sql.ResultSet.*;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;

public class AerospikeConnection implements Connection, SimpleWrapper {

    private static final Logger logger = Logger.getLogger(AerospikeConnection.class.getName());

    private final String url;
    private final IAerospikeClient client;
    private volatile boolean readOnly = false;
    private final Properties clientInfo = new Properties();
    private volatile Map<String, Class<?>> typeMap = emptyMap();
    private volatile int holdability = HOLD_CURSORS_OVER_COMMIT;
    private final AtomicReference<String> schema = new AtomicReference<>(null); // namespace
    private volatile boolean closed;

    public AerospikeConnection(String url, Properties props) {
        this.url = url;
        URLParser.parseUrl(url, props);
        client = new AerospikeClient(
                URLParser.getClientPolicy(), URLParser.getHosts()
        );
        schema.set(URLParser.getSchema()); // namespace
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
        throw new SQLFeatureNotSupportedException("nativeSQL is not supported");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        // do nothing
    }

    @Override
    public boolean getAutoCommit() {
        return true;
    }

    @Override
    public void commit() {
        // do nothing
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("rollback is not supported");
    }

    @Override
    public void close() {
        client.close();
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() {
        logger.info("getMetaData request");
        return new AerospikeDatabaseMetadata(url, client, this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (!isValid(1)) {
            throw new SQLException("Cannot set read only mode on closed connection");
        }
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) {
        schema.set(catalog);
    }

    @Override
    public String getCatalog() {
        return schema.get();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (level != TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException(format("Aerospike does not support transactions," +
                    " so the only valid value here is TRANSACTION_NONE=%d", TRANSACTION_NONE));
        }
    }

    @Override
    public int getTransactionIsolation() {
        return TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
        // TODO make use of warnings
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, holdability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, holdability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql, resultSetType, resultSetConcurrency, holdability);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        return typeMap;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) {
        typeMap = map;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Cannot set holdability on closed connection");
        }
        if (!(holdability == HOLD_CURSORS_OVER_COMMIT || holdability == CLOSE_CURSORS_AT_COMMIT)) {
            throw new SQLException(format(
                    "Unsupported holdability %d. Must be either HOLD_CURSORS_OVER_COMMIT=%d or CLOSE_CURSORS_AT_COMMIT=%d",
                    holdability,
                    HOLD_CURSORS_OVER_COMMIT,
                    CLOSE_CURSORS_AT_COMMIT));
        }

        this.holdability = holdability;
    }

    @Override
    public int getHoldability() {
        return holdability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection is not transactional");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection is not transactional");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection is not transactional");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection is not transactional");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        validateResultSetParameters(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new AerospikeStatement(client, this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        validateResultSetParameters(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new AerospikePreparedStatement(client, this, sql);
    }

    private void validateResultSetParameters(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetType != TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("ResultSet type other than TYPE_FORWARD_ONLY is not supported");
        }
        if (resultSetConcurrency != CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Updatable ResultSet is not supported yet");
        }
        if (!(resultSetHoldability == HOLD_CURSORS_OVER_COMMIT || resultSetHoldability == CLOSE_CURSORS_AT_COMMIT)) {
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
    public Clob createClob() {
        return new StringClob();
    }

    @Override
    public Blob createBlob() {
        return new ByteArrayBlob();
    }

    @Override
    public NClob createNClob() {
        return new StringClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML is not supported");
    }

    @Override
    public boolean isValid(int timeout) {
        return client.isConnected() && Objects.nonNull(client.getClusterStats());
    }

    @Override
    public void setClientInfo(String name, String value) {
        clientInfo.setProperty(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) {
        clientInfo.putAll(properties);
    }

    @Override
    public String getClientInfo(String name) {
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() {
        return clientInfo;
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
    public void setSchema(String schema) {
        // do nothing
    }

    @Override
    public String getSchema() {
        return schema.get();
    }

    @Override
    public void abort(Executor executor) {
        executor.execute(this::close);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
        stream(new Policy[]{
                client.getReadPolicyDefault(),
                client.getWritePolicyDefault(),
                client.getScanPolicyDefault(),
                client.getQueryPolicyDefault(),
                client.getBatchPolicyDefault()
        }).forEach(p -> p.totalTimeout = milliseconds);
        client.getInfoPolicyDefault().timeout = milliseconds;
    }

    @Override
    public int getNetworkTimeout() {
        return client.getReadPolicyDefault().totalTimeout;
    }

}
