package com.aerospike.jdbc.schema;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.jdbc.model.DataColumn;
import com.aerospike.jdbc.model.DriverPolicy;
import com.aerospike.jdbc.model.SchemaTableName;

import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import static com.aerospike.jdbc.util.Constants.DEFAULT_SCHEMA_NAME;
import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;

public final class AerospikeSchemaBuilder {

    private static final Logger logger = Logger.getLogger(AerospikeSchemaBuilder.class.getName());

    private final IAerospikeClient client;
    private final AerospikeSchemaCache schemaCache;
    private final int scanMaxRecords;

    public AerospikeSchemaBuilder(IAerospikeClient client, DriverPolicy driverPolicy) {
        this.client = client;
        schemaCache = new AerospikeSchemaCache(Duration.ofSeconds(driverPolicy.getMetadataCacheTtlSeconds()));
        scanMaxRecords = driverPolicy.getSchemaBuilderMaxRecords();
    }

    public List<DataColumn> getSchema(SchemaTableName schemaTableName) {
        return schemaCache.get(schemaTableName).orElseGet(() -> {
            logger.info(() -> "Fetching SchemaTableName: " + schemaTableName);
            final Map<String, DataColumn> columnHandles = new TreeMap<>(String::compareToIgnoreCase);
            ScanPolicy policy = new ScanPolicy(client.getScanPolicyDefault());
            policy.maxRecords = scanMaxRecords;

            // add record key column handler
            columnHandles.put(PRIMARY_KEY_COLUMN_NAME,
                    new DataColumn(
                            schemaTableName.getSchemaName(),
                            schemaTableName.getTableName(),
                            Types.VARCHAR,
                            PRIMARY_KEY_COLUMN_NAME,
                            PRIMARY_KEY_COLUMN_NAME));

            client.scanAll(policy, schemaTableName.getSchemaName(), toSet(schemaTableName.getTableName()),
                    (key, rec) -> {
                        Map<String, Object> bins = rec.bins;
                        if (bins != null) {
                            bins.forEach((k, value) -> {
                                logger.fine(() -> String.format("Bin: %s -> %s", k, value));
                                int t = getBinType(value);
                                if (k != null && t != 0) {
                                    columnHandles.put(k, new DataColumn(schemaTableName.getSchemaName(),
                                            schemaTableName.getTableName(), t, k, k));
                                }
                            });
                        }
                    });

            List<DataColumn> columns = new ArrayList<>(columnHandles.values());
            schemaCache.put(schemaTableName, columns);
            return columns;
        });
    }

    private String toSet(String tableName) {
        if (tableName.equals(DEFAULT_SCHEMA_NAME)) {
            return null;
        }
        return tableName;
    }

    private int getBinType(Object value) {
        int t = 0;
        if (value instanceof byte[] || value instanceof Value.BytesValue || value instanceof Value.ByteSegmentValue) {
            t = Types.VARBINARY;
        } else if (value instanceof String || value instanceof Value.StringValue) {
            t = Types.VARCHAR;
        } else if (value instanceof Integer || value instanceof Value.IntegerValue) {
            t = Types.INTEGER;
        } else if (value instanceof Long || value instanceof Value.LongValue) {
            t = Types.BIGINT;
        } else if (value instanceof Double || value instanceof Value.DoubleValue) {
            t = Types.DOUBLE;
        } else if (value instanceof Float || value instanceof Value.FloatValue) {
            t = Types.FLOAT;
        } else if (value instanceof Boolean || value instanceof Value.BooleanValue) {
            t = Types.BOOLEAN;
        } else if (value instanceof Byte || value instanceof Value.ByteValue) {
            t = Types.TINYINT;
        } else if (value instanceof Value.HLLValue) {
            t = Types.BLOB;
        } else if (value instanceof Value.GeoJSONValue) {
            t = Types.BLOB;
        } else if (value instanceof List<?> || value instanceof Value.ListValue) {
            t = Types.ARRAY;
        } else if (value instanceof Map<?, ?> || value instanceof Value.MapValue) {
            t = Types.OTHER;
        } else {
            logger.info(() -> "Unknown bin type: " + value);
        }
        return t;
    }
}
