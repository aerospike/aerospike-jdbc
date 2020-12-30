package com.aerospike.jdbc.schema;

import com.aerospike.jdbc.model.DataColumn;
import com.aerospike.jdbc.model.SchemaTableName;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public final class AerospikeSchemaCache
        implements OptionalCache<SchemaTableName, List<DataColumn>> {

    private final Cache<SchemaTableName, List<DataColumn>> store;

    public AerospikeSchemaCache(Duration ttl) {
        store = CacheBuilder.newBuilder().expireAfterWrite(ttl).build();
    }

    @Override
    public Optional<List<DataColumn>> get(SchemaTableName schemaTableName) {
        return Optional.ofNullable(store.getIfPresent(schemaTableName));
    }

    @Override
    public void put(SchemaTableName schemaTableName, List<DataColumn> columns) {
        store.put(schemaTableName, columns);
    }
}
