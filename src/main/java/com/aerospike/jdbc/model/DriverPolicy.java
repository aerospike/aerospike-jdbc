package com.aerospike.jdbc.model;

import java.util.Properties;

public class DriverPolicy {

    private static final int DEFAULT_CAPACITY = 256;
    private static final int DEFAULT_TIMEOUT_MS = 1000;
    private static final int DEFAULT_METADATA_CACHE_TTL_SECONDS = 3600;

    private final int recordSetQueueCapacity;
    private final int recordSetTimeoutMs;
    private final int metadataCacheTtlSeconds;

    public DriverPolicy(Properties properties) {
        recordSetQueueCapacity = parseInt(properties.getProperty("recordSetQueueCapacity"), DEFAULT_CAPACITY);
        recordSetTimeoutMs = parseInt(properties.getProperty("recordSetTimeoutMs"), DEFAULT_TIMEOUT_MS);
        metadataCacheTtlSeconds = parseInt(properties.getProperty("metadataCacheTtlSeconds"),
                DEFAULT_METADATA_CACHE_TTL_SECONDS);
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

    private int parseInt(String value, int defaultValue) {
        if (value != null) {
            return Integer.parseInt(value);
        }
        return defaultValue;
    }
}
