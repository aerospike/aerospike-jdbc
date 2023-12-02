package com.aerospike.jdbc.sql;

import com.aerospike.client.Value;
import com.aerospike.jdbc.model.DataColumn;

import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static com.aerospike.jdbc.util.SqlLiterals.sqlToJavaTypes;
import static com.aerospike.jdbc.util.SqlLiterals.sqlTypeNames;

public class AerospikeResultSetMetaData implements ResultSetMetaData, SimpleWrapper {

    protected static final Map<Integer, Integer> precisionByType = new HashMap<>();
    private static final Logger logger = Logger.getLogger(AerospikeResultSetMetaData.class.getName());

    private static final int MAX_BLOCK_SIZE = 128 * 1024;
    private static final int MAX_DATE_SIZE = 32;

    static {
        precisionByType.put(Types.BIT, 8);
        precisionByType.put(Types.TINYINT, 8);
        precisionByType.put(Types.SMALLINT, 8);
        precisionByType.put(Types.INTEGER, 8);
        precisionByType.put(Types.BIGINT, 8);
        precisionByType.put(Types.FLOAT, 8);
        precisionByType.put(Types.REAL, 8);
        precisionByType.put(Types.DOUBLE, 8);
        precisionByType.put(Types.NUMERIC, 8);
        precisionByType.put(Types.DECIMAL, 8);
        precisionByType.put(Types.CHAR, 2);
        precisionByType.put(Types.VARCHAR, MAX_BLOCK_SIZE);
        precisionByType.put(Types.LONGVARCHAR, MAX_BLOCK_SIZE);
        precisionByType.put(Types.DATE, MAX_DATE_SIZE);
        precisionByType.put(Types.TIME, MAX_DATE_SIZE);
        precisionByType.put(Types.TIMESTAMP, MAX_DATE_SIZE);
        precisionByType.put(Types.BINARY, MAX_BLOCK_SIZE);
        precisionByType.put(Types.VARBINARY, MAX_BLOCK_SIZE);
        precisionByType.put(Types.LONGVARBINARY, MAX_BLOCK_SIZE);
        precisionByType.put(Types.NULL, 0);
        precisionByType.put(Types.OTHER, MAX_BLOCK_SIZE);
        precisionByType.put(Types.JAVA_OBJECT, MAX_BLOCK_SIZE);
        precisionByType.put(Types.DISTINCT, MAX_BLOCK_SIZE);
        precisionByType.put(Types.STRUCT, MAX_BLOCK_SIZE);
        precisionByType.put(Types.ARRAY, MAX_BLOCK_SIZE);
        precisionByType.put(Types.BLOB, MAX_BLOCK_SIZE);
        precisionByType.put(Types.CLOB, MAX_BLOCK_SIZE);
        precisionByType.put(Types.REF, 0); // ??
        precisionByType.put(Types.DATALINK, 0); // ??
        if (Value.UseBoolBin) {
            precisionByType.put(Types.BOOLEAN, 1);
        } else {
            precisionByType.put(Types.BOOLEAN, 8);
        }
        precisionByType.put(Types.ROWID, 0); // ??
        precisionByType.put(Types.NCHAR, 2);
        precisionByType.put(Types.NVARCHAR, MAX_BLOCK_SIZE);
        precisionByType.put(Types.LONGNVARCHAR, MAX_BLOCK_SIZE);
        precisionByType.put(Types.NCLOB, MAX_BLOCK_SIZE);
        precisionByType.put(Types.SQLXML, MAX_BLOCK_SIZE);
        precisionByType.put(Types.REF_CURSOR, 0); // ??
        precisionByType.put(Types.TIME_WITH_TIMEZONE, MAX_DATE_SIZE);
        precisionByType.put(Types.TIMESTAMP_WITH_TIMEZONE, MAX_DATE_SIZE);
    }

    private final String schema;
    private final String table;
    private final List<DataColumn> columns; // has the order of the inferred schema

    public AerospikeResultSetMetaData(String schema, String table, List<DataColumn> columns) {
        this.schema = schema;
        this.table = table;
        this.columns = Collections.unmodifiableList(columns);
    }

    public List<DataColumn> getColumns() {
        return columns;
    }

    private Stream<DataColumn> getVisibleColumns() {
        return columns.stream();
    }

    @Override
    public int getColumnCount() {
        logger.fine(() -> "getColumnCount: " + getVisibleColumns().count());
        return (int) getVisibleColumns().count();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return true;
    }

    @Override
    public boolean isSearchable(int column) {
        return true;
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public int isNullable(int column) {
        return columnNullable; // any column in Aerospike is nullable
    }

    @Override
    public boolean isSigned(int column) {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return 32; // just to return something that > 0. It is difficult to estimate real display size
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        logger.fine(() -> "getColumnName: " + column);
        validateColumn(column);
        return columns.get(column - 1).getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        validateColumn(column);
        return schema;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return precisionByType.getOrDefault(getColumnType(column), 0);
    }

    @Override
    public int getScale(int column) {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        validateColumn(column);
        return table;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return getSchemaName(column); // return schema
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        validateColumn(column);
        return columns.get(column - 1).getType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return sqlTypeNames.get(getColumnType(column));
    }

    @Override
    public boolean isReadOnly(int column) {
        return false;
    }

    @Override
    public boolean isWritable(int column) {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return true;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return sqlToJavaTypes.get(getColumnType(column)).getName();
    }

    private void validateColumn(int column) throws SQLException {
        if (columns.size() < column) {
            throw new SQLDataException(String.format("Column %d is out of range", column));
        }
    }
}
