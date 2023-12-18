package com.aerospike.jdbc.schema;

import com.aerospike.jdbc.model.CatalogTableName;
import com.aerospike.jdbc.model.DataColumn;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public final class AerospikeSchemaCache
        implements OptionalCache<CatalogTableName, List<DataColumn>> {

    private final Cache<CatalogTableName, List<DataColumn>> store;

    public AerospikeSchemaCache(Duration ttl) {
        store = CacheBuilder.newBuilder().expireAfterWrite(ttl).build();
    }

    @Override
    public Optional<List<DataColumn>> get(CatalogTableName catalogTableName) {
        return Optional.ofNullable(store.getIfPresent(catalogTableName));
    }

    @Override
    public void put(CatalogTableName catalogTableName, List<DataColumn> columns) {
        store.put(catalogTableName, columns);
    }

    @Override
    public void clear() {
        store.invalidateAll();
    }
}
