package com.aerospike.jdbc.util;

public final class Constants {

    public static final String defaultKeyName = "__key";
    public static final String defaultSchemaName = "__default";

    public static final long schemaScanRecords = 1000L;
    public static final long schemaCacheTTLMinutes = 30L;

    public static final int defaultQueryLimit = 10000;

    private Constants() {
    }
}
