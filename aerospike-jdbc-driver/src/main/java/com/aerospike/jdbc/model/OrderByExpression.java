package com.aerospike.jdbc.model;

import java.util.List;

public class OrderByExpression {

    public enum Ordering {
        DESC, ASC
    }

    private List<String> columns;
    private Ordering ordering;

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setOrdering(Ordering ordering) {
        this.ordering = ordering;
    }

    public Ordering getOrdering() {
        return ordering;
    }
}
