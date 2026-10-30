package com.aerospike.jdbc.query;

import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.QueryType;
import org.apache.calcite.sql.parser.SqlParseException;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class BatchCommandTest {

    @Test
    public void testOfSingleRowInsert() throws SqlParseException {
        AerospikeQuery query = AerospikeQuery.parse("INSERT INTO jdbc (__key, int1) VALUES ('k1', 1)", null);
        BatchCommand command = BatchCommand.of(query);

        assertSame(command.getQuery(), query);
        assertEquals(command.getQueryType(), QueryType.INSERT);
        assertEquals(command.getRows(), Collections.singletonList(Arrays.asList("k1", 1L)));
    }

    @Test
    public void testOfMultiRowInsert() throws SqlParseException {
        AerospikeQuery query = AerospikeQuery.parse(
                "INSERT INTO jdbc (__key, int1) VALUES ('k1', 1), ('k2', 2)", null);
        BatchCommand command = BatchCommand.of(query);

        assertEquals(command.getRows(), Arrays.asList(Arrays.asList("k1", 1L), Arrays.asList("k2", 2L)));
        assertEquals(command.getRows(), query.getValues());
    }

    @Test
    public void testOfParameterizedInsert() throws SqlParseException {
        AerospikeQuery query = AerospikeQuery.parse("INSERT INTO jdbc (__key, int1) VALUES (?, ?)",
                Arrays.asList("k1", 7));
        BatchCommand command = BatchCommand.of(query);

        assertEquals(command.getRows(), Collections.singletonList(Arrays.asList("k1", 7)));
    }

    @Test
    public void testOfNonInsertHasNoRows() throws SqlParseException {
        AerospikeQuery update = AerospikeQuery.parse("UPDATE jdbc SET int1=1 WHERE __key='k1'", null);
        AerospikeQuery delete = AerospikeQuery.parse("DELETE FROM jdbc WHERE __key='k1'", null);

        assertNull(BatchCommand.of(update).getRows());
        assertNull(BatchCommand.of(delete).getRows());
    }

    @Test
    public void testInsertRow() throws SqlParseException {
        AerospikeQuery template = AerospikeQuery.parse("INSERT INTO jdbc (__key, int1) VALUES (?, ?)",
                Arrays.asList("k1", 1));
        List<Object> row = Arrays.asList("k2", 2);
        BatchCommand command = BatchCommand.insertRow(template, row);

        assertSame(command.getQuery(), template);
        assertEquals(command.getRows().size(), 1);
        assertSame(command.getRows().get(0), row);
    }
}
