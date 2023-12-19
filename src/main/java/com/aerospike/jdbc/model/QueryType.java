package com.aerospike.jdbc.model;

public enum QueryType {
    SHOW_CATALOGS,
    SHOW_SCHEMAS,
    SHOW_TABLES,
    SHOW_COLUMNS,
    DROP_SCHEMA,
    DROP_TABLE,
    CREATE_INDEX,
    DROP_INDEX,
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    UNKNOWN
}
