package com.aerospike.jdbc;

import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.AerospikeSqlVisitor;
import com.aerospike.jdbc.model.QueryType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.testng.Assert.*;

public class QueryParserTest {

    @Test
    public void testSelectQuery() throws SqlParseException {
        SqlParser parser = SqlParser.create(
                "select pkup_datetime, vendor_id from \"test.nyc-data\" where id=112279922 and trip_distance=5.79 or trip_type is not null " +
                        "and cab_type='green' and archived=false limit 10 offset 5", AerospikeQuery.sqlParserConfig);
        SqlNode parsed = parser.parseQuery();
        AerospikeQuery query = parsed.accept(new AerospikeSqlVisitor());

        assertEquals(query.getQueryType(), QueryType.SELECT);
        assertEquals(query.getSchema(), "test");
        assertEquals(query.getTable(), "nyc-data");
        assertEquals(query.getColumns(), Arrays.asList("pkup_datetime", "vendor_id"));
        assertNotNull(query.getPredicate());
        assertEquals(query.getLimit(), 10);
        assertEquals(query.getOffset(), 5);
    }

    @Test
    public void testSelectCountQuery() throws SqlParseException {
        SqlParser parser = SqlParser.create(
                "select count(*) from \"test.nyc-data\" where archived=false", AerospikeQuery.sqlParserConfig);
        SqlNode parsed = parser.parseQuery();
        AerospikeQuery query = parsed.accept(new AerospikeSqlVisitor());

        assertEquals(query.getQueryType(), QueryType.SELECT);
        assertEquals(query.getSchema(), "test");
        assertEquals(query.getTable(), "nyc-data");
        assertEquals(query.getColumns(), Collections.singletonList("COUNT(*)"));
        assertNotNull(query.getPredicate());
    }

    @Test
    public void testUpdateQuery() throws SqlParseException {
        SqlParser parser = SqlParser.create(
                "update \"test.nyc-data\" set archived=true where id=112279922 and trip_distance=5.79 or trip_type is not null " +
                        "and cab_type='green'", AerospikeQuery.sqlParserConfig);
        SqlNode parsed = parser.parseQuery();
        AerospikeQuery query = parsed.accept(new AerospikeSqlVisitor());

        assertEquals(query.getQueryType(), QueryType.UPDATE);
        assertEquals(query.getSchema(), "test");
        assertEquals(query.getTable(), "nyc-data");
        assertEquals(query.getColumns(), Collections.singletonList("archived"));
        assertEquals(query.getValues(), Collections.singletonList(true));
        assertNotNull(query.getPredicate());
    }

    @Test
    public void testInsertQuery() throws SqlParseException {
        SqlParser parser = SqlParser.create(
                "insert into \"test.nyc-data\" (id, cab_type, trip_distance, archived) values " +
                        "(112279922, 'green', 2.75, false), (112279923, \"yellow\", 5.0, true)", AerospikeQuery.sqlParserConfig);
        SqlNode parsed = parser.parseQuery();
        AerospikeQuery query = parsed.accept(new AerospikeSqlVisitor());

        assertEquals(query.getQueryType(), QueryType.INSERT);
        assertEquals(query.getSchema(), "test");
        assertEquals(query.getTable(), "nyc-data");
        assertEquals(query.getColumns(), Arrays.asList("id", "cab_type", "trip_distance", "archived"));
        assertEquals(query.getValues().size(), 2);
        assertNull(query.getPredicate());
    }

    @Test
    public void testDeleteQuery() throws SqlParseException {
        SqlParser parser = SqlParser.create(
                "delete from \"test.nyc-data\" where id=112279922 and not trip_distance=5.79 or trip_type is null " +
                        "and cab_type like '%green'", AerospikeQuery.sqlParserConfig);
        SqlNode parsed = parser.parseQuery();
        AerospikeQuery query = parsed.accept(new AerospikeSqlVisitor());

        assertEquals(query.getQueryType(), QueryType.DELETE);
        assertEquals(query.getSchema(), "test");
        assertEquals(query.getTable(), "nyc-data");
        assertNotNull(query.getPredicate());
    }

    @Test
    public void testDropTableQuery() throws SqlParseException {
        SqlParser parser = SqlParser.create(
                "drop table \"test.nyc-data\"", AerospikeQuery.sqlParserConfig);
        SqlNode parsed = parser.parseQuery();
        AerospikeQuery query = parsed.accept(new AerospikeSqlVisitor());

        assertEquals(query.getQueryType(), QueryType.DROP_TABLE);
        assertEquals(query.getSchema(), "test");
        assertEquals(query.getTable(), "nyc-data");
        assertNull(query.getPredicate());
    }

    @Test
    public void testDropSchemaQuery() throws SqlParseException {
        SqlParser parser = SqlParser.create(
                "drop schema test", AerospikeQuery.sqlParserConfig);
        SqlNode parsed = parser.parseQuery();
        AerospikeQuery query = parsed.accept(new AerospikeSqlVisitor());

        assertEquals(query.getQueryType(), QueryType.DROP_SCHEMA);
        assertEquals(query.getSchema(), "test");
        assertNull(query.getTable());
        assertNull(query.getPredicate());
    }

    @Test
    public void testSelectInQuery() throws SqlParseException {
        SqlParser parser = SqlParser.create(
                "select trip_distance from \"test.nyc-data\" where id in (1234, 1235)", AerospikeQuery.sqlParserConfig);
        SqlNode parsed = parser.parseQuery();
        AerospikeQuery query = parsed.accept(new AerospikeSqlVisitor());

        assertEquals(query.getQueryType(), QueryType.SELECT);
        assertEquals(query.getSchema(), "test");
        assertEquals(query.getTable(), "nyc-data");
        assertNotNull(query.getPredicate());
        assertEquals(query.getColumns(), Collections.singletonList("trip_distance"));
    }

    @Test
    public void testSelectBetweenQuery() throws SqlParseException {
        SqlParser parser = SqlParser.create(
                "select trip_distance from \"test.nyc-data\" where id between 1234 and 1245", AerospikeQuery.sqlParserConfig);
        SqlNode parsed = parser.parseQuery();
        AerospikeQuery query = parsed.accept(new AerospikeSqlVisitor());

        assertEquals(query.getQueryType(), QueryType.SELECT);
        assertEquals(query.getSchema(), "test");
        assertEquals(query.getTable(), "nyc-data");
        assertNotNull(query.getPredicate());
        assertEquals(query.getColumns(), Collections.singletonList("trip_distance"));
    }
}
