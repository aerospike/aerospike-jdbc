package com.aerospike.jdbc.sql;

import com.aerospike.client.Record;
import com.aerospike.jdbc.async.RecordSet;
import com.aerospike.jdbc.model.DataColumn;

import java.math.BigDecimal;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

import static com.aerospike.jdbc.util.Constants.defaultKeyName;

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
        if (columnLabel.equals(defaultKeyName)) {
            return recordSet.getKey().userKey;
        }
        return recordSet.getRecord().bins.get(columnLabel);
    }

    @Override
    public String getString(String columnLabel) {
        logger.fine(() -> "getString: " + columnLabel);
        if (columnLabel.equals(defaultKeyName)) {
            return recordSet.getKey().userKey.toString();
        }
        Object bin = recordSet.getRecord().bins.get(columnLabel);
        return Objects.isNull(bin) ? null : bin.toString();
    }

    @Override
    public boolean getBoolean(String columnLabel) {
        logger.fine(() -> "getBoolean: " + columnLabel);
        if (columnLabel.equals(defaultKeyName)) {
            return Boolean.parseBoolean(recordSet.getKey().userKey.toString());
        }
        return Boolean.parseBoolean(recordSet.getRecord().bins.get(columnLabel).toString());
    }

    @Override
    public byte getByte(String columnLabel) {
        logger.fine(() -> "getByte: " + columnLabel);
        return 0;
    }

    @Override
    public short getShort(String columnLabel) {
        logger.fine(() -> "getShort: " + columnLabel);
        return (short) getInt(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) {
        logger.fine(() -> "getInt: " + columnLabel);
        if (columnLabel.equals(defaultKeyName)) {
            return recordSet.getKey().userKey.toInteger();
        }
        return Integer.parseInt(recordSet.getRecord().bins.get(columnLabel).toString());
    }

    @Override
    public long getLong(String columnLabel) {
        logger.fine(() -> "getLong: " + columnLabel);
        if (columnLabel.equals(defaultKeyName)) {
            return recordSet.getKey().userKey.toLong();
        }
        return Long.parseLong(recordSet.getRecord().bins.get(columnLabel).toString());
    }

    @Override
    public float getFloat(String columnLabel) {
        logger.fine(() -> "getFloat: " + columnLabel);
        return (float) getDouble(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) {
        logger.fine(() -> "getDouble: " + columnLabel);
        if (columnLabel.equals(defaultKeyName)) {
            return Double.parseDouble(recordSet.getKey().userKey.toString());
        }
        return Double.parseDouble(recordSet.getRecord().bins.get(columnLabel).toString());
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) {
        logger.fine(() -> "getBigDecimal: " + columnLabel);
        return BigDecimal.valueOf(getLong(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) {
        logger.fine(() -> "getBytes: " + columnLabel);
        if (columnLabel.equals(defaultKeyName)) {
            return recordSet.getKey().userKey.toString().getBytes();
        }
        return (byte[]) recordSet.getRecord().bins.get(columnLabel);
    }
}
