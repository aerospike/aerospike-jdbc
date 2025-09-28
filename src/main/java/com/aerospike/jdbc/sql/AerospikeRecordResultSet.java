package com.aerospike.jdbc.sql;

import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.jdbc.async.RecordSet;
import com.aerospike.jdbc.model.DataColumn;
import com.google.common.io.BaseEncoding;

import java.math.BigDecimal;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.aerospike.jdbc.util.Constants.METADATA_DIGEST_COLUMN_NAME;
import static com.aerospike.jdbc.util.Constants.METADATA_GEN_COLUMN_NAME;
import static com.aerospike.jdbc.util.Constants.METADATA_TTL_COLUMN_NAME;
import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;

public class AerospikeRecordResultSet extends BaseResultSet<Record> {

    private static final Logger logger = Logger.getLogger(AerospikeRecordResultSet.class.getName());

    private final RecordSet recordSet;
    private final Set<String> columnNames;

    public AerospikeRecordResultSet(
            RecordSet recordSet,
            Statement statement,
            String catalog,
            String table,
            List<DataColumn> columns // columns list
    ) {
        super(statement, catalog, table, columns);
        this.recordSet = recordSet;
        this.columnNames = columns.stream().map(DataColumn::getName).collect(Collectors.toSet());
    }

    @Override
    protected Record getRecord() {
        return recordSet.getRecord();
    }

    @Override
    protected boolean moveToNext() {
        return recordSet.next();
    }

    @Override
    public Object getObject(String columnLabel) {
        logger.fine(() -> "getObject: " + columnLabel);
        Object obj = getValue(columnLabel).map(Value::getObject).orElse(null);
        wasNull = obj == null;
        return obj;
    }

    @Override
    public String getString(String columnLabel) {
        logger.fine(() -> "getString: " + columnLabel);
        String str = getValue(columnLabel).map(Value::toString).orElse(null);
        wasNull = str == null;
        return str;
    }

    @Override
    public boolean getBoolean(String columnLabel) {
        logger.fine(() -> "getBoolean: " + columnLabel);
        return getValue(columnLabel).map(Value::toString).map(Boolean::parseBoolean).orElse(false);
    }

    @Override
    public byte getByte(String columnLabel) {
        logger.fine(() -> "getByte: " + columnLabel);
        return (byte) getInt(columnLabel);
    }

    @Override
    public short getShort(String columnLabel) {
        logger.fine(() -> "getShort: " + columnLabel);
        return (short) getInt(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) {
        logger.fine(() -> "getInt: " + columnLabel);
        return getValue(columnLabel).map(Value::toString).map(Integer::parseInt).orElse(0);
    }

    @Override
    public long getLong(String columnLabel) {
        logger.fine(() -> "getLong: " + columnLabel);
        return getValue(columnLabel).map(Value::toString).map(Long::parseLong).orElse(0L);
    }

    @Override
    public float getFloat(String columnLabel) {
        logger.fine(() -> "getFloat: " + columnLabel);
        return (float) getDouble(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) {
        logger.fine(() -> "getDouble: " + columnLabel);
        return getValue(columnLabel).map(Value::toString).map(Double::parseDouble).orElse(0.0d);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) {
        logger.fine(() -> "getBigDecimal: " + columnLabel);
        return BigDecimal.valueOf(getLong(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) {
        logger.fine(() -> "getBytes: " + columnLabel);
        return getValue(columnLabel).map(Value::getObject).map(byte[].class::cast).orElse(null);
    }

    @Override
    public void close() {
        super.close();
        cancel();
    }

    public void cancel() {
        recordSet.invalidate();
    }

    private Optional<Value> getValue(String columnLabel) {
        if (!columnNames.contains(columnLabel)) {
            return Optional.empty();
        }
        switch (columnLabel) {
            case PRIMARY_KEY_COLUMN_NAME:
                return Optional.ofNullable(recordSet.getKey())
                        .map(key -> key.userKey);
            case METADATA_DIGEST_COLUMN_NAME:
                return Optional.ofNullable(recordSet.getKey())
                        .map(key -> BaseEncoding.base16().lowerCase().encode(key.digest))
                        .map(Value::get);
            case METADATA_TTL_COLUMN_NAME:
                return Optional.ofNullable(recordSet.getRecord())
                        .map(rec -> rec.expiration)
                        .map(Value::get);
            case METADATA_GEN_COLUMN_NAME:
                return Optional.ofNullable(recordSet.getRecord())
                        .map(rec -> rec.generation)
                        .map(Value::get);
            default: // regular bin value
                return Optional.ofNullable(recordSet.getRecord())
                        .map(rec -> rec.bins.get(columnLabel))
                        .map(Value::get);
        }
    }
}
