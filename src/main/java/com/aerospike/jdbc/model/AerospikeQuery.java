package com.aerospike.jdbc.model;

import com.aerospike.jdbc.predicate.QueryPredicate;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import java.util.Collections;
import java.util.List;

public class AerospikeQuery {

    @VisibleForTesting
    public static final SqlParser.Config sqlParserConfig = SqlParser.config()
            .withParserFactory(SqlDdlParserImpl.FACTORY)
            .withCaseSensitive(true)
            .withUnquotedCasing(Casing.UNCHANGED)
            .withQuotedCasing(Casing.UNCHANGED);

    private String catalog;
    private String schema;
    private String table;
    private QueryType queryType;
    private Integer offset;
    private Integer limit;

    private QueryPredicate predicate;
    private List<Object> values;
    private List<String> columns;

    public AerospikeQuery() {
        this.queryType = QueryType.UNKNOWN;
    }

    public static AerospikeQuery parse(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, sqlParserConfig);
        SqlNode parsed = parser.parseQuery();
        return parsed.accept(new AerospikeSqlVisitor());
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

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    public QueryType getQueryType() {
        return queryType;
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

    public void setPredicate(QueryPredicate predicate) {
        this.predicate = predicate;
    }

    public QueryPredicate getPredicate() {
        return predicate;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
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

    public List<Object> getPrimaryKeys() {
        if (predicate != null) {
            return predicate.getPrimaryKeys();
        }
        return Collections.emptyList();
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
