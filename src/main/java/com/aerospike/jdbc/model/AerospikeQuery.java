package com.aerospike.jdbc.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class AerospikeQuery {

    private String catalog;
    private String schema;
    private String table;
    private QueryType type;
    private final List<OrderByExpression> orderBy;
    private Integer offset;
    private Integer limit;
    private Boolean distinct;
    private WhereExpression where;
    private String like;
    private String escape;
    private List<String> values;
    private List<String> columns;

    public AerospikeQuery() {
        this.type = QueryType.UNKNOWN;
        this.orderBy = new ArrayList<>();
        this.values = new LinkedList<>();
        this.columns = new LinkedList<>();
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public void setSchema(String schema) {
        this.catalog = schema; // TODO ?
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }

    public void setTable(String table) {
        String[] spec = table.split("\\.");
        switch (spec.length) {
            case 3:
                this.catalog = spec[0];
                this.schema = spec[1];
                this.table = spec[2];
                break;
            case 2:
                this.schema = spec[0];
                this.table = spec[1];
                break;
            case 1:
                this.table = spec[0];
                break;
            default:
                throw new IllegalArgumentException("Invalid table name");
        }
    }

    public String getTable() {
        return table;
    }

    public SchemaTableName getSchemaTable() {
        return new SchemaTableName(schema, table);
    }

    public void setType(QueryType type) {
        this.type = type;
    }

    public QueryType getType() {
        return type;
    }

    public void appendOrderBy(OrderByExpression orderBy) {
        this.orderBy.add(orderBy);
    }

    public List<OrderByExpression> getOrderBy() {
        return orderBy;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public Boolean getDistinct() {
        return distinct;
    }

    public void setWhere(WhereExpression where) {
        this.where = where;
    }

    public WhereExpression getWhere() {
        return where;
    }

    public void setLike(String like) {
        this.like = like;
    }

    public String getLike() {
        return like;
    }

    public void setEscape(String escape) {
        this.escape = escape;
    }

    public String getEscape() {
        return escape;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public void appendValues(String... values) {
        this.values.addAll(Arrays.stream(values).filter(x -> !x.equals("")).collect(Collectors.toList()));
    }

    public List<String> getValues() {
        return values;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public void appendColumns(String... columns) {
        this.columns.addAll(Arrays.stream(columns).filter(x -> !x.equals("")).collect(Collectors.toList()));
    }

    public List<String> getColumns() {
        return columns;
    }

    public String[] getBinNames() {
        if (columns.size() == 1 && columns.get(0).equals("*")) {
            return null;
        }
        return columns.toArray(new String[0]);
    }

    @Override
    public String toString() {
        try {
            return (new ObjectMapper()).writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return getClass().getName() + "@" + Integer.toHexString(hashCode());
        }
    }
}
