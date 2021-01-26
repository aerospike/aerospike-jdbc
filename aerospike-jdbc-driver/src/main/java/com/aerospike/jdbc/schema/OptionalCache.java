package com.aerospike.jdbc.schema;

import java.util.Optional;

public interface OptionalCache<K, V>
{
    public Optional<V> get(K key);

    public void put(K key, V value);

    public void clear();
}
