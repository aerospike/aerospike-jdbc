package com.aerospike.jdbc.util;

public final class Constants {

    public static final String PRIMARY_KEY_COLUMN_NAME = "__key";
    public static final String DEFAULT_SCHEMA_NAME = "__default";

    public static final String METADATA_DIGEST_COLUMN_NAME = "__digest";
    public static final String METADATA_TTL_COLUMN_NAME = "__ttl";
    public static final String METADATA_GEN_COLUMN_NAME = "__gen";

    public static final String UNSUPPORTED_QUERY_TYPE_MESSAGE = "Unsupported query type";

    // Driver version
    public static final String DRIVER_VERSION = "2.0.0";
    public static final int DRIVER_MAJOR_VERSION = 2;
    public static final int DRIVER_MINOR_VERSION = 0;

    // JDBC specification
    public static final String JDBC_VERSION = "4.2";
    public static final int JDBC_MAJOR_VERSION = JDBC_VERSION.charAt(0) - '0';
    public static final int JDBC_MINOR_VERSION = JDBC_VERSION.charAt(2) - '0';

    private Constants() {
    }
}
