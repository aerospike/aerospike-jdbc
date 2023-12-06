package com.aerospike.jdbc.sql;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public interface IndexToLabelResultSet extends ResultSet {

    @Override
    default String getString(int columnIndex) throws SQLException {
        return getString(getColumnLabel(columnIndex));
    }

    @Override
    default boolean getBoolean(int columnIndex) throws SQLException {
        return getBoolean(getColumnLabel(columnIndex));
    }

    @Override
    default byte getByte(int columnIndex) throws SQLException {
        return getByte(getColumnLabel(columnIndex));
    }

    @Override
    default short getShort(int columnIndex) throws SQLException {
        return getShort(getColumnLabel(columnIndex));
    }

    @Override
    default int getInt(int columnIndex) throws SQLException {
        return getInt(getColumnLabel(columnIndex));
    }

    @Override
    default long getLong(int columnIndex) throws SQLException {
        return getLong(getColumnLabel(columnIndex));
    }

    @Override
    default float getFloat(int columnIndex) throws SQLException {
        return getFloat(getColumnLabel(columnIndex));
    }

    @Override
    default double getDouble(int columnIndex) throws SQLException {
        return getDouble(getColumnLabel(columnIndex));
    }

    @Override
    default BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(getColumnLabel(columnIndex)).setScale(scale, RoundingMode.FLOOR);
    }

    @Override
    default byte[] getBytes(int columnIndex) throws SQLException {
        return getBytes(getColumnLabel(columnIndex));
    }

    @Override
    default Date getDate(int columnIndex) throws SQLException {
        return getDate(getColumnLabel(columnIndex));
    }

    @Override
    default Time getTime(int columnIndex) throws SQLException {
        return getTime(getColumnLabel(columnIndex));
    }

    @Override
    default Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(getColumnLabel(columnIndex));
    }

    @Override
    default InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getAsciiStream(getColumnLabel(columnIndex));
    }

    /**
     * @deprecated use <code>getCharacterStream</code> in place of <code>getUnicodeStream</code>
     */
    @Override
    @Deprecated
    @SuppressWarnings("java:S1133")
    default InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getUnicodeStream(getColumnLabel(columnIndex));
    }

    @Override
    default InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getBinaryStream(getColumnLabel(columnIndex));
    }

    @Override
    default Object getObject(int columnIndex) throws SQLException {
        return getObject(getColumnLabel(columnIndex));
    }

    @Override
    default Reader getCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(getColumnLabel(columnIndex));
    }

    @Override
    default BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(getColumnLabel(columnIndex));
    }

    @Override
    default Blob getBlob(int columnIndex) throws SQLException {
        return getBlob(getColumnLabel(columnIndex));
    }

    @Override
    default Clob getClob(int columnIndex) throws SQLException {
        return getClob(getColumnLabel(columnIndex));
    }

    @Override
    default Array getArray(int columnIndex) throws SQLException {
        return getArray(getColumnLabel(columnIndex));
    }

    @Override
    default Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(getColumnLabel(columnIndex), cal);
    }

    @Override
    default Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(getColumnLabel(columnIndex), cal);
    }

    @Override
    default Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(getColumnLabel(columnIndex), cal);
    }

    @Override
    default URL getURL(int columnIndex) throws SQLException {
        return getURL(getColumnLabel(columnIndex));
    }

    @Override
    default RowId getRowId(int columnIndex) throws SQLException {
        return getRowId(getColumnLabel(columnIndex));
    }

    @Override
    default NClob getNClob(int columnIndex) throws SQLException {
        return getNClob(getColumnLabel(columnIndex));
    }

    @Override
    default SQLXML getSQLXML(int columnIndex) throws SQLException {
        return getSQLXML(getColumnLabel(columnIndex));
    }

    @Override
    default String getNString(int columnIndex) throws SQLException {
        return getNString(getColumnLabel(columnIndex));
    }

    @Override
    default Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getNCharacterStream(getColumnLabel(columnIndex));
    }

    @Override
    default <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return getObject(getColumnLabel(columnIndex), type);
    }

    default Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(getColumnLabel(columnIndex), map);
    }

    @Override
    default Ref getRef(int columnIndex) throws SQLException {
        return getRef(getColumnLabel(columnIndex));
    }

    default String getColumnLabel(int columnIndex) throws SQLException {
        return getMetaData().getColumnLabel(columnIndex);
    }
}
