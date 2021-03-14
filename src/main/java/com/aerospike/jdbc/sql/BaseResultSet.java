package com.aerospike.jdbc.sql;

import com.aerospike.jdbc.model.DataColumn;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import static java.lang.String.format;

public abstract class BaseResultSet<T> implements ResultSet,
        IndexToLabelResultSet, UpdateResultSet, SimpleWrapper {

    private static final Logger logger = Logger.getLogger(BaseResultSet.class.getName());

    private final Statement statement;
    protected final String schema;
    protected final String table;
    private boolean wasNull = false;
    protected volatile int index;
    private volatile boolean closed;

    protected boolean afterLast = false;

    protected final List<DataColumn> columns;
    private final ResultSetMetaData metadata;

    public BaseResultSet(Statement statement, String schema, String table, List<DataColumn> columns) {
        this.statement = statement;
        this.schema = schema;
        this.table = table;
        this.columns = Collections.unmodifiableList(columns);
        this.metadata = new AerospikeResultSetMetaData(schema, table, columns);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return metadata;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return IntStream.range(0, columns.size())
                .filter(i -> columnLabel.equals(columns.get(i).getName()))
                .map(i -> i + 1)
                .findFirst()
                .orElseThrow(() -> new SQLException(format("Column %s does not exist", columnLabel)));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return BigDecimal.valueOf(getDouble(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() {
        return index == 0;
    }

    @Override
    public boolean isAfterLast() {
        return afterLast;
    }

    @Override
    public boolean isFirst() {
        return index == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (index != 0) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public void afterLast() throws SQLException {
        while (next()) ;
        afterLast = true;
    }

    @Override
    public boolean first() throws SQLException {
        if (index == 0) {
            return next();
        }
        throw new SQLException("Cannot rewind result set");
    }

    protected abstract T getRecord();

    @Override
    public boolean last() throws SQLException {
        if (isAfterLast()) {
            return false;
        }
        T lastRecord;
        do {
            lastRecord = getRecord();
        } while (next());

        if (lastRecord != null) {
            afterLast = false;
            setCurrentRecord(lastRecord);
            return true;
        }
        return false;
    }

    protected void setCurrentRecord(T record) {
        // default empty implementation
    }

    @Override
    public int getRow() {
        return afterLast ? 0 : index;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return relative(row - index);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        int n = Math.abs(rows);
        for (int i = 0; i < n; i++) {
            if (!next()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void setFetchSize(int rows) {
        // fetch size of 1 supported
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBlob"); // TODO check cast
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getNClob(columnLabel);
    }

    @Override
    public Array getArray(String columnLabel) {
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        try {
            String spec = getString(columnLabel);
            return spec != null ? new URL(spec) : null;
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) {
        return getCharacterStream(columnLabel);
    }

    @Override
    public <U> U getObject(String columnLabel, Class<U> type) {
        return null;
    }

    protected void assertClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Result set is closed");
        }
    }

    protected abstract boolean moveToNext();

    @Override
    public boolean next() throws SQLException {
        synchronized (this) {
            assertClosed();
            boolean result = moveToNext();
            if (result) {
                clearWarnings();
                index++;
            } else {
                afterLast = true;
            }
            return result;
        }
    }
}
