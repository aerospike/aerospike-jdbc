package com.aerospike.jdbc;

import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.QueryType;
import com.aerospike.jdbc.util.AuxStatementParser;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class QueryCustomParserTest {

    private static final String tableName = "jdbc";

    @Test
    public void testIndexCreateQuery() throws SQLException {
        AerospikeQuery query;

        String lowCaseQuery = format("create index str1_idx on %s (str1);", tableName);
        query = AuxStatementParser.parse(lowCaseQuery);
        assertIndexCreateQuery(query);

        String quotedTableQuery = format("create index str1_idx on \"%s\" (str1);", tableName);
        query = AuxStatementParser.parse(quotedTableQuery);
        assertIndexCreateQuery(query);

        String whiteSpacesQuery = format("create  index str1_idx    on %s( str1  ) ;", tableName);
        query = AuxStatementParser.parse(whiteSpacesQuery);
        assertIndexCreateQuery(query);
    }

    private void assertIndexCreateQuery(AerospikeQuery query) {
        assertEquals(query.getQueryType(), QueryType.CREATE_INDEX);
        assertEquals(query.getTable(), tableName);
        assertEquals(query.getIndex(), "str1_idx");
        assertEquals(query.getColumns().get(0), "str1");
    }

    @Test
    public void testIndexDropQuery() throws SQLException {
        AerospikeQuery query;

        String lowCaseQuery = format("drop index str1_idx on %s;", tableName);
        query = AuxStatementParser.parse(lowCaseQuery);
        assertIndexDropQuery(query);

        String quotedTableQuery = format("drop index str1_idx on \"%s\";", tableName);
        query = AuxStatementParser.parse(quotedTableQuery);
        assertIndexDropQuery(query);

        String whiteSpacesQuery = format("drop  index    str1_idx on  %s  ;", tableName);
        query = AuxStatementParser.parse(whiteSpacesQuery);
        assertIndexDropQuery(query);
    }

    private void assertIndexDropQuery(AerospikeQuery query) {
        assertEquals(query.getQueryType(), QueryType.DROP_INDEX);
        assertEquals(query.getTable(), tableName);
        assertEquals(query.getIndex(), "str1_idx");
    }

    @Test
    public void testTruncateTableQuery() throws SQLException {
        AerospikeQuery query;

        String lowCaseQuery = format("truncate table %s", tableName);
        query = AuxStatementParser.parse(lowCaseQuery);
        assertTruncateTableQuery(query);

        String quotedTableQuery = format("truncate table \"%s\"", tableName);
        query = AuxStatementParser.parse(quotedTableQuery);
        assertTruncateTableQuery(query);

        String whiteSpacesQuery = format("truncate   table   %s  ;", tableName);
        query = AuxStatementParser.parse(whiteSpacesQuery);
        assertTruncateTableQuery(query);
    }

    private void assertTruncateTableQuery(AerospikeQuery query) {
        assertEquals(query.getQueryType(), QueryType.DROP_TABLE);
        assertEquals(query.getTable(), tableName);
    }
}
