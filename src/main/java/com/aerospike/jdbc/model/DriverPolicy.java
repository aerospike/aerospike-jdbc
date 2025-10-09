package com.aerospike.jdbc.model;

import java.util.Properties;

public class DriverPolicy {

    private static final int DEFAULT_RECORD_SET_QUEUE_CAPACITY = 256;
    private static final int DEFAULT_RECORD_SET_TIMEOUT_MS = 1000;
    private static final int DEFAULT_METADATA_CACHE_TTL_SECONDS = 3600;
    private static final int DEFAULT_SCHEMA_BUILDER_MAX_RECORDS = 1000;

    private final int recordSetQueueCapacity;
    private final int recordSetTimeoutMs;
    private final int metadataCacheTtlSeconds;
    private final int schemaBuilderMaxRecords;
    private final int txnTimeoutSeconds;
    private final int queryLimit;
    private final boolean showRecordMetadata;
    private final boolean refuseScan;

    public DriverPolicy(Properties properties) {
        recordSetQueueCapacity = parseInt(properties.getProperty("recordSetQueueCapacity"),
                DEFAULT_RECORD_SET_QUEUE_CAPACITY);
        recordSetTimeoutMs = parseInt(properties.getProperty("recordSetTimeoutMs"),
                DEFAULT_RECORD_SET_TIMEOUT_MS);
        metadataCacheTtlSeconds = parseInt(properties.getProperty("metadataCacheTtlSeconds"),
                DEFAULT_METADATA_CACHE_TTL_SECONDS);
        schemaBuilderMaxRecords = parseInt(properties.getProperty("schemaBuilderMaxRecords"),
                DEFAULT_SCHEMA_BUILDER_MAX_RECORDS);
        txnTimeoutSeconds = parseInt(properties.getProperty("txnTimeoutSeconds"), 0);
        queryLimit = parseInt(properties.getProperty("queryLimit"), 0);
        showRecordMetadata = Boolean.parseBoolean(properties.getProperty("showRecordMetadata"));
        refuseScan = Boolean.parseBoolean(properties.getProperty("refuseScan"));
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

    public int getTxnTimeoutSeconds() {
        return txnTimeoutSeconds;
    }

    public int getQueryLimit() {
        return queryLimit;
    }

    public boolean getShowRecordMetadata() {
        return showRecordMetadata;
    }

    public boolean getRefuseScan() {
        return refuseScan;
    }

    private int parseInt(String value, int defaultValue) {
        if (value != null) {
            return Integer.parseInt(value);
        }
        return defaultValue;
    }
}
