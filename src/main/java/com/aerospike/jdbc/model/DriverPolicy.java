package com.aerospike.jdbc.model;

import java.util.Properties;

public class DriverPolicy {

    private static final int DEFAULT_CAPACITY = 256;
    private static final int DEFAULT_TIMEOUT_MS = 1000;
    private static final int DEFAULT_METADATA_CACHE_TTL_SECONDS = 3600;
    private static final int DEFAULT_SCHEMA_BUILDER_MAX_RECORDS = 1000;

    private final int recordSetQueueCapacity;
    private final int recordSetTimeoutMs;
    private final int metadataCacheTtlSeconds;
    private final int schemaBuilderMaxRecords;

    public DriverPolicy(Properties properties) {
        recordSetQueueCapacity = parseInt(properties.getProperty("recordSetQueueCapacity"), DEFAULT_CAPACITY);
        recordSetTimeoutMs = parseInt(properties.getProperty("recordSetTimeoutMs"), DEFAULT_TIMEOUT_MS);
        metadataCacheTtlSeconds = parseInt(properties.getProperty("metadataCacheTtlSeconds"),
                DEFAULT_METADATA_CACHE_TTL_SECONDS);
        schemaBuilderMaxRecords = parseInt(properties.getProperty("schemaBuilderMaxRecords"),
                DEFAULT_SCHEMA_BUILDER_MAX_RECORDS);
    }

    public int getRecordSetQueueCapacity() {
        return recordSetQueueCapacity;
    }

    public int getRecordSetTimeoutMs() {
        return recordSetTimeoutMs;
    }

    public int getMetadataCacheTtlSeconds() {
        return metadataCacheTtlSeconds;
    }

    public int getSchemaBuilderMaxRecords() {
        return schemaBuilderMaxRecords;
    }

    private int parseInt(String value, int defaultValue) {
        if (value != null) {
            return Integer.parseInt(value);
        }
        return defaultValue;
    }
}
