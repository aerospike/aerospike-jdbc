package com.aerospike.jdbc;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.Pair;
import com.aerospike.jdbc.model.QueryType;
import com.aerospike.jdbc.query.QueryPerformer;
import com.aerospike.jdbc.sql.SimpleWrapper;
import com.aerospike.jdbc.util.AuxStatementParser;
import org.apache.calcite.sql.parser.SqlParseException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

public class AerospikeStatement implements Statement, SimpleWrapper {

    private static final Logger logger = Logger.getLogger(AerospikeStatement.class.getName());
    private static final String BATCH_NOT_SUPPORTED_MESSAGE = "Batch update is not supported";
    private static final String AUTO_GENERATED_KEYS_NOT_SUPPORTED_MESSAGE = "Auto-generated keys are not supported";

    protected final IAerospikeClient client;
    protected final AerospikeConnection connection;

    protected String schema;
    protected ResultSet resultSet;
    protected int updateCount;

    private int maxRows = Integer.MAX_VALUE;
    private int queryTimeout;

    public AerospikeStatement(IAerospikeClient client, AerospikeConnection connection) {
        this.client = client;
        this.connection = connection;
        this.schema = connection.getCatalog();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        logger.info(() -> "executeQuery: " + sql);
        AerospikeQuery query = parseQuery(sql);
        runQuery(query);
        return resultSet;
    }

    protected void runQuery(AerospikeQuery query) {
        Pair<ResultSet, Integer> result = QueryPerformer.executeQuery(client, this, query);
        resultSet = result.getLeft();
        updateCount = result.getRight();
    }

    protected AerospikeQuery parseQuery(String sql) throws SQLException {
        sql = sql.replace("\n", " ");
        AerospikeQuery query;
        try {
            query = AerospikeQuery.parse(sql);
        } catch (SqlParseException e) {
            query = AuxStatementParser.hack(sql);
        }
        if (query.getSchema() == null) {
            query.setSchema(schema);
        }
        return query;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        executeQuery(sql);
        return updateCount;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public int getMaxFieldSize() {
        return 8 * 1024 * 1024; // 8 MB
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException("Max field size cannot be changed dynamically");
    }

    @Override
    public int getMaxRows() {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) {
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) {
        // do nothing
    }

    @Override
    public int getQueryTimeout() {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) {
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement cannot be canceled");
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
        // do nothing
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Named cursor is not supported");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        logger.info(() -> "execute: " + sql);
        AerospikeQuery query = parseQuery(sql);
        runQuery(query);
        return query.getQueryType() == QueryType.SELECT;
    }

    @Override
    public ResultSet getResultSet() {
        return resultSet;
    }

    @Override
    public int getUpdateCount() {
        return updateCount;
    }

    @Override
    public boolean getMoreResults() {
        return false;
    }

    @Override
    public int getFetchDirection() {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            throw new SQLException(format("Attempt to set unsupported fetch direction %d. " +
                    "Only FETCH_FORWARD=%d is supported. The value is ignored.", direction, FETCH_FORWARD));
        }
    }

    @Override
    public int getFetchSize() {
        return 1;
    }

    @Override
    public void setFetchSize(int rows) {
        // do nothing supported size = 1.
    }

    @Override
    public int getResultSetConcurrency() {
        return CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException(BATCH_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException(BATCH_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException(BATCH_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
            return executeUpdate(sql);
        }
        throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        if (columnIndexes == null || columnIndexes.length == 0) {
            return executeUpdate(sql);
        }
        throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        if (columnNames == null || columnNames.length == 0) {
            return executeUpdate(sql);
        }
        throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
            return execute(sql);
        }
        throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        if (columnIndexes == null || columnIndexes.length == 0) {
            return execute(sql);
        }
        throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        if (columnNames == null || columnNames.length == 0) {
            return execute(sql);
        }
        throw new SQLFeatureNotSupportedException(AUTO_GENERATED_KEYS_NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public int getResultSetHoldability() {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (poolable) {
            throw new SQLFeatureNotSupportedException("Statement does not support pools");
        }
    }

    @Override
    public void closeOnCompletion() {
        // do nothing
    }

    @Override
    public boolean isCloseOnCompletion() {
        return false;
    }
}
