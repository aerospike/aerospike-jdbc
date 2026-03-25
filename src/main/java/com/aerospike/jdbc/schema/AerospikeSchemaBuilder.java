package com.aerospike.jdbc.schema;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.jdbc.model.CatalogTableName;
import com.aerospike.jdbc.model.DataColumn;
import com.aerospike.jdbc.model.DriverPolicy;
import com.aerospike.jdbc.model.Pair;

import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import static com.aerospike.jdbc.util.Constants.DEFAULT_SCHEMA_NAME;
import static com.aerospike.jdbc.util.Constants.METADATA_DIGEST_COLUMN_NAME;
import static com.aerospike.jdbc.util.Constants.METADATA_GEN_COLUMN_NAME;
import static com.aerospike.jdbc.util.Constants.METADATA_TTL_COLUMN_NAME;
import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;

public final class AerospikeSchemaBuilder {

    private static final Logger logger = Logger.getLogger(AerospikeSchemaBuilder.class.getName());

    private final IAerospikeClient client;
    private final DriverPolicy driverPolicy;
    private final AerospikeSchemaCache schemaCache;

    public AerospikeSchemaBuilder(IAerospikeClient client, DriverPolicy driverPolicy) {
        this.client = client;
        this.driverPolicy = driverPolicy;
        schemaCache = new AerospikeSchemaCache(Duration.ofSeconds(driverPolicy.getMetadataCacheTtlSeconds()));
    }

    public List<DataColumn> getSchema(CatalogTableName catalogTableName) {
        return schemaCache.get(catalogTableName).orElseGet(() -> {
            logger.info(() -> "Fetching CatalogTableName: " + catalogTableName);
            final Map<String, DataColumn> columnHandles = initColumnHandles(catalogTableName);

            ScanPolicy policy = new ScanPolicy(client.getScanPolicyDefault());
            policy.maxRecords = driverPolicy.getSchemaBuilderMaxRecords();

            client.scanAll(policy, catalogTableName.getCatalogName(), toSet(catalogTableName.getTableName()),
                    (key, rec) -> {
                        if (key != null && key.userKey != null) {
                            columnHandles.put(PRIMARY_KEY_COLUMN_NAME,
                                    new DataColumn(catalogTableName.getCatalogName(),
                                            catalogTableName.getTableName(),
                                            getBinType(key.userKey.getObject()),
                                            PRIMARY_KEY_COLUMN_NAME,
                                            PRIMARY_KEY_COLUMN_NAME));
                        }
                        Map<String, Object> bins = rec.bins;
                        if (bins != null) {
                            bins.forEach((k, value) -> {
                                logger.fine(() -> String.format("Bin: %s -> %s", k, value));
                                int t = getBinType(value);
                                if (k != null && t != 0) {
                                    columnHandles.put(k, new DataColumn(catalogTableName.getCatalogName(),
                                            catalogTableName.getTableName(), t, k, k));
                                }
                            });
                        }
                    });

            List<DataColumn> columns = new ArrayList<>(columnHandles.values());
            schemaCache.put(catalogTableName, columns);
            return columns;
        });
    }

    private Map<String, DataColumn> initColumnHandles(CatalogTableName catalogTableName) {
        final Map<String, DataColumn> columnHandles = new TreeMap<>(String::compareToIgnoreCase);
        final List<Pair<String, Integer>> metadataColumns = new ArrayList<>();

        // add record key column handler
        metadataColumns.add(Pair.of(PRIMARY_KEY_COLUMN_NAME, Types.VARCHAR));

        // add record metadata column handlers
        if (driverPolicy.getShowRecordMetadata()) {
            metadataColumns.addAll(Arrays.asList(
                    Pair.of(METADATA_DIGEST_COLUMN_NAME, Types.VARCHAR),
                    Pair.of(METADATA_GEN_COLUMN_NAME, Types.INTEGER),
                    Pair.of(METADATA_TTL_COLUMN_NAME, Types.INTEGER)
            ));
        }

        for (Pair<String, Integer> md : metadataColumns) {
            columnHandles.put(md.getLeft(),
                    new DataColumn(
                            catalogTableName.getCatalogName(),
                            catalogTableName.getTableName(),
                            md.getRight(),
                            md.getLeft(),
                            md.getLeft()));
        }
        return columnHandles;
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
