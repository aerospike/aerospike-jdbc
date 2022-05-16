package com.aerospike.jdbc.predicate;

@FunctionalInterface
interface VarArgsFunction<T, R> {
    @SuppressWarnings("unchecked")
    R apply(T... args);
}
