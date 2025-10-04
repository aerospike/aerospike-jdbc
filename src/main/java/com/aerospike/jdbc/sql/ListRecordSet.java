package com.aerospike.jdbc.sql;

import com.aerospike.jdbc.model.DataColumn;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;

public class ListRecordSet extends BaseResultSet<List<?>> {

    private static final Logger logger = Logger.getLogger(ListRecordSet.class.getName());

    private final Iterator<List<?>> it;
    private final Map<String, Integer> nameToIndex;
    private List<?> currentRecord = null;

    public ListRecordSet(Statement statement, String catalog, String table, List<DataColumn> columns,
                         Iterable<List<?>> data) {
        super(statement, catalog, table, columns);
        this.it = data.iterator();
        nameToIndex = IntStream.range(0, columns.size()).boxed()
                .collect(toMap(i -> columns.get(i).getName(), i -> i));
    }

    @Override
    protected List<?> getRecord() {
        return currentRecord;
    }

    @Override
    public Object getObject(String columnLabel) {
        return getRecord().get(nameToIndex.get(columnLabel));
    }

    @Override
    public String getString(String columnLabel) {
        Object value = getObject(columnLabel);
        return Objects.isNull(value) ? null : value.toString();
    }

    @Override
    public boolean getBoolean(String columnLabel) {
        return Boolean.parseBoolean(getObject(columnLabel).toString());
    }

    @Override
    public byte getByte(String columnLabel) {
        return (byte) getObject(columnLabel);
    }

    @Override
    public short getShort(String columnLabel) {
        return (short) getObject(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) {
        String strVal = null;
        try {
            strVal = getObject(columnLabel).toString();
            return Integer.parseInt(strVal);
        } catch (Exception e) {
            logger.warning("getInt Exception for " + columnLabel + ", " + strVal);
            return -1;
        }
    }

    @Override
    public long getLong(String columnLabel) {
        String strVal = null;
        try {
            strVal = getObject(columnLabel).toString();
            return Long.parseLong(strVal);
        } catch (Exception e) {
            logger.warning("getLong Exception for " + columnLabel + ", " + strVal);
            return -1;
        }
    }

    @Override
    public float getFloat(String columnLabel) {
        return (float) getObject(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) {
        return (double) getObject(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBigDecimal");
    }

    @Override
    public byte[] getBytes(String columnLabel) {
        return (byte[]) getObject(columnLabel);
    }

    @Override
    public boolean isLast() {
        return !it.hasNext();
    }

    @Override
    protected boolean moveToNext() {
        boolean hasNext = it.hasNext();
        currentRecord = hasNext ? it.next() : null;
        return hasNext;
    }

    @Override
    protected void setCurrentRecord(List<?> rec) {
        currentRecord = rec;
    }
}
