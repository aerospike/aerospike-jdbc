package com.aerospike.jdbc.model;

import java.util.Properties;

public class DriverPolicy {

    private static final int DEFAULT_CAPACITY = 256;
    private static final int DEFAULT_TIMEOUT = 1000;

    private final int recordSetQueueCapacity;
    private final int recordSetTimeoutMs;

    public DriverPolicy(Properties properties) {
        recordSetQueueCapacity = parseInt(properties.getProperty("recordSetQueueCapacity"), DEFAULT_CAPACITY);
        recordSetTimeoutMs = parseInt(properties.getProperty("recordSetTimeoutMs"), DEFAULT_TIMEOUT);
    }

    public int getRecordSetQueueCapacity() {
        return recordSetQueueCapacity;
    }

    public int getRecordSetTimeoutMs() {
        return recordSetTimeoutMs;
    }

    private int parseInt(String value, int defaultValue) {
        if (value != null) {
            return Integer.parseInt(value);
        }
        return defaultValue;
    }
}
