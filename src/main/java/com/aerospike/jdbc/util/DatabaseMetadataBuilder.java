package com.aerospike.jdbc.util;

import com.aerospike.jdbc.AerospikeConnection;
import com.aerospike.jdbc.AerospikeDatabaseMetadata;
import com.aerospike.jdbc.model.DriverPolicy;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class DatabaseMetadataBuilder {

    private final Cache<String, AerospikeDatabaseMetadata> metadataCache;

    public DatabaseMetadataBuilder(DriverPolicy driverPolicy) {
        metadataCache = CacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(driverPolicy.getMetadataCacheTtlSeconds()))
                .build();
    }

    public AerospikeDatabaseMetadata build(String url, AerospikeConnection connection)
            throws SQLException {
        try {
            return metadataCache.get(url, () -> new AerospikeDatabaseMetadata(url, connection));
        } catch (ExecutionException e) {
            throw new SQLException(e);
        }
    }
}
