package com.aerospike.jdbc.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.jdbc.AerospikeDatabaseMetadata;
import com.aerospike.jdbc.model.DriverPolicy;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.sql.Connection;
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

    public AerospikeDatabaseMetadata build(String url, IAerospikeClient client, Connection connection)
            throws SQLException {
        try {
            return metadataCache.get(url, () -> new AerospikeDatabaseMetadata(url, client, connection));
        } catch (ExecutionException e) {
            throw new SQLException(e);
        }
    }
}
