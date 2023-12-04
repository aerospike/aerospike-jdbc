package com.aerospike.jdbc.util;

public final class Constants {

    public static final String PRIMARY_KEY_COLUMN_NAME = "__key";
    public static final String DEFAULT_SCHEMA_NAME = "__default";

    public static final long schemaScanRecords = 1000L;
    public static final long schemaCacheTTLMinutes = 30L;

    public static final String UNSUPPORTED_QUERY_TYPE_MESSAGE = "Unsupported query type";

    private Constants() {
    }
}
