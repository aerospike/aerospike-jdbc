package com.aerospike.jdbc.sql;

import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.jdbc.async.RecordSet;
import com.aerospike.jdbc.model.DataColumn;

import java.math.BigDecimal;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;

public class AerospikeRecordResultSet extends BaseResultSet<Record> {

    private static final Logger logger = Logger.getLogger(AerospikeRecordResultSet.class.getName());

    private final RecordSet recordSet;

    public AerospikeRecordResultSet(
            RecordSet recordSet,
            Statement statement,
            String schema,
            String table,
            List<DataColumn> columns // columns list
    ) {
        super(statement, schema, table, columns);
        this.recordSet = recordSet;
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
        Object obj;
        if (columnLabel.equals(PRIMARY_KEY_COLUMN_NAME)) {
            obj = getUserKey().map(Value::getObject).orElse(null);
        } else {
            obj = getBin(columnLabel).orElse(null);
        }
        wasNull = obj == null;
        return obj;
    }

    @Override
    public String getString(String columnLabel) {
        logger.fine(() -> "getString: " + columnLabel);
        String str;
        if (columnLabel.equals(PRIMARY_KEY_COLUMN_NAME)) {
            str = getUserKey().map(Value::toString).orElse(null);
        } else {
            str = getBin(columnLabel).map(Object::toString).orElse(null);
        }
        wasNull = str == null;
        return str;
    }

    @Override
    public boolean getBoolean(String columnLabel) {
        logger.fine(() -> "getBoolean: " + columnLabel);
        if (columnLabel.equals(PRIMARY_KEY_COLUMN_NAME)) {
            return getUserKey().map(Value::toString).map(Boolean::parseBoolean).orElse(false);
        }
        return getBin(columnLabel).map(Object::toString).map(Boolean::parseBoolean).orElse(false);
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
        if (columnLabel.equals(PRIMARY_KEY_COLUMN_NAME)) {
            return getUserKey().map(Value::toInteger).orElse(0);
        }
        return getBin(columnLabel).map(Object::toString).map(Integer::parseInt).orElse(0);
    }

    @Override
    public long getLong(String columnLabel) {
        logger.fine(() -> "getLong: " + columnLabel);
        if (columnLabel.equals(PRIMARY_KEY_COLUMN_NAME)) {
            return getUserKey().map(Value::toLong).orElse(0L);
        }
        return getBin(columnLabel).map(Object::toString).map(Long::parseLong).orElse(0L);
    }

    @Override
    public float getFloat(String columnLabel) {
        logger.fine(() -> "getFloat: " + columnLabel);
        return (float) getDouble(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) {
        logger.fine(() -> "getDouble: " + columnLabel);
        if (columnLabel.equals(PRIMARY_KEY_COLUMN_NAME)) {
            return getUserKey().map(Value::toString).map(Double::parseDouble).orElse(0.0d);
        }
        return getBin(columnLabel).map(Object::toString).map(Double::parseDouble).orElse(0.0d);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) {
        logger.fine(() -> "getBigDecimal: " + columnLabel);
        return BigDecimal.valueOf(getLong(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) {
        logger.fine(() -> "getBytes: " + columnLabel);
        if (columnLabel.equals(PRIMARY_KEY_COLUMN_NAME)) {
            return getUserKey().map(Value::toString).map(String::getBytes).orElse(null);
        }
        return getBin(columnLabel).map(byte[].class::cast).orElse(null);
    }

    private Optional<Value> getUserKey() {
        return Optional.ofNullable(recordSet.getKey().userKey);
    }

    private Optional<Object> getBin(String columnLabel) {
        return Optional.ofNullable(recordSet.getRecord().bins.get(columnLabel));
    }
}
