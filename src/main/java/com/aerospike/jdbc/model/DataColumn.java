package com.aerospike.jdbc.model;

import java.util.Objects;

public class DataColumn {

    private final String name;
    private String catalog;
    private String table;
    private String label;
    private int type;

    public DataColumn(String catalog, String table, String name, String label) {
        this.catalog = catalog;
        this.table = table;
        this.name = name;
        this.label = label;
    }

    public DataColumn(String catalog, String table, int type, String name, String label) {
        this(catalog, table, name, label);
        this.type = type;
    }

    public DataColumn withCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public DataColumn withTable(String table) {
        this.table = table;
        return this;
    }

    public DataColumn withType(int type) {
        this.type = type;
        return this;
    }

    public DataColumn withLabel(String label) {
        this.label = label;
        return this;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getTable() {
        return table;
    }

    public String getName() {
        return name;
    }

    public String getLabel() {
        return label;
    }

    public int getType() {
        return type;
    }

    public CatalogTableName getSchemaTableName() {
        return new CatalogTableName(catalog, table);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataColumn that = (DataColumn) o;
        return type == that.type &&
                Objects.equals(catalog, that.catalog) &&
                Objects.equals(table, that.table) &&
                Objects.equals(name, that.name) &&
                Objects.equals(label, that.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, table, name, label, type);
    }

    @Override
    public String toString() {
        return String.format("%s(%s, %s, %s, %s, %d)",
                getClass().getSimpleName(), name, catalog, table, label, type);
    }
}
