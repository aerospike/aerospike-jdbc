package com.aerospike.jdbc.schema;

import java.util.Optional;

public interface OptionalCache<K, V> {

    Optional<V> get(K key);

    void put(K key, V value);

    void clear();
}
