package com.aerospike.jdbc.sql;

import java.sql.SQLException;
import java.sql.Wrapper;

public interface SimpleWrapper extends Wrapper {
    @Override
    default <T> T unwrap(Class<T> iface) throws SQLException {
        // The implementation is taken from MySQL driver
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException e) {
            throw new SQLException("Cannot unwrap " + iface, e);
        }
    }

    @Override
    default boolean isWrapperFor(Class<?> iface) {
        // The implementation is taken from MySQL driver
        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }
}
