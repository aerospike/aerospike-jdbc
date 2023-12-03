package com.aerospike.jdbc.schema;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.jdbc.model.DataColumn;
import com.aerospike.jdbc.model.SchemaTableName;

import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import static com.aerospike.jdbc.util.Constants.defaultKeyName;
import static com.aerospike.jdbc.util.Constants.defaultSchemaName;
import static com.aerospike.jdbc.util.Constants.schemaCacheTTLMinutes;
import static com.aerospike.jdbc.util.Constants.schemaScanRecords;

public final class AerospikeSchemaBuilder {

    private static final Logger logger = Logger.getLogger(AerospikeSchemaBuilder.class.getName());

    private static final Duration cacheTTL = Duration.ofMinutes(schemaCacheTTLMinutes);
    private static final AerospikeSchemaCache cache = new AerospikeSchemaCache(cacheTTL);

    private AerospikeSchemaBuilder() {
    }

    public static void cleanSchemaCache() {
        cache.clear();
    }

    public static List<DataColumn> getSchema(SchemaTableName schemaTableName, IAerospikeClient client) {
        return cache.get(schemaTableName).orElseGet(() -> {
            logger.info(() -> "Fetching SchemaTableName: " + schemaTableName);
            final Map<String, DataColumn> columnHandles = new TreeMap<>(String::compareToIgnoreCase);
            ScanPolicy policy = new ScanPolicy(client.getScanPolicyDefault());
            policy.maxRecords = schemaScanRecords;

            // add record key column handler
            columnHandles.put(defaultKeyName,
                    new DataColumn(schemaTableName.getSchemaName(),
                            schemaTableName.getTableName(), Types.VARCHAR, defaultKeyName, defaultKeyName));

            client.scanAll(policy, schemaTableName.getSchemaName(), toSet(schemaTableName.getTableName()), (key, rec) -> {
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
            cache.put(schemaTableName, columns);
            return columns;
        });
    }

    private static String toSet(String tableName) {
        if (tableName.equals(defaultSchemaName)) {
            return null;
        }
        return tableName;
    }

    private static int getBinType(Object value) {
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
            t = Types.STRUCT;
        } else {
            logger.info(() -> "Unknown bin type: " + value);
        }
        return t;
    }
}
