package com.aerospike.jdbc.model;

import java.util.Objects;

public class CatalogTableName {

    private final String catalogName;
    private final String tableName;

    public CatalogTableName(String catalogName, String tableName) {
        this.catalogName = catalogName;
        this.tableName = tableName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CatalogTableName that = (CatalogTableName) o;
        return Objects.equals(catalogName, that.catalogName) &&
                Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, tableName);
    }

    @Override
    public String toString() {
        return String.format("%s(%s, %s)", getClass().getSimpleName(), catalogName, tableName);
    }
}
