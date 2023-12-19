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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static com.aerospike.jdbc.util.Constants.DEFAULT_SCHEMA_NAME;
import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;

public class AerospikeQuery {

    @VisibleForTesting
    public static final SqlParser.Config sqlParserConfig = SqlParser.config()
            .withParserFactory(SqlDdlParserImpl.FACTORY)
            .withCaseSensitive(true)
            .withUnquotedCasing(Casing.UNCHANGED)
            .withQuotedCasing(Casing.UNCHANGED);

    private static final String ASTERISK = "*";

    private String catalog;
    private String table;
    private QueryType queryType;
    private Integer offset;
    private Integer limit;
    private String index;

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

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        String[] spec = table.split("\\.");
        switch (spec.length) {
            case 2:
                this.catalog = spec[0];
                this.table = spec[1];
                break;
            case 1:
                this.table = spec[0];
                break;
            default:
                throw new IllegalArgumentException("Invalid table name");
        }
    }

    public String getSetName() {
        if (table.equals(DEFAULT_SCHEMA_NAME)) {
            return null;
        }
        return table;
    }

    public CatalogTableName getCatalogTable() {
        return new CatalogTableName(catalog, table);
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public QueryPredicate getPredicate() {
        return predicate;
    }

    public void setPredicate(QueryPredicate predicate) {
        this.predicate = predicate;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public String[] columnBins() {
        String[] binNames = columns.stream()
                .filter(c -> !Objects.equals(c, ASTERISK))
                .filter(c -> !Objects.equals(c, PRIMARY_KEY_COLUMN_NAME))
                .toArray(String[]::new);
        return binNames.length == 0 ? null : binNames;
    }

    public Collection<Object> getPrimaryKeys() {
        if (predicate != null) {
            return predicate.getPrimaryKeys();
        }
        return Collections.emptyList();
    }

    public boolean isPrimaryKeyOnly() {
        return columns.size() == 1 && columns.get(0).equals(PRIMARY_KEY_COLUMN_NAME);
    }

    public boolean isStar() {
        return columns.stream().anyMatch(c -> c.equals(ASTERISK));
    }

    public boolean isCount() {
        return columns.size() == 1 && columns.get(0).toLowerCase(Locale.ENGLISH).startsWith("count(");
    }

    public boolean isIndexable() {
        return Objects.nonNull(predicate) && predicate.isIndexable() && Objects.isNull(offset);
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
