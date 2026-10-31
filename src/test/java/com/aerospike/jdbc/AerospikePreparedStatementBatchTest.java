package com.aerospike.jdbc;

import com.aerospike.jdbc.model.AerospikeQuery;
import org.apache.calcite.sql.parser.SqlParseException;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static com.aerospike.jdbc.AerospikePreparedStatement.isSingleRowParametersInsert;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AerospikePreparedStatementBatchTest {

    @Test
    public void testAllDirectParameters() throws SqlParseException {
        assertTrue(check("INSERT INTO jdbc (__key, int1, l1) VALUES (?, ?, ?)",
                Arrays.asList("k1", 1L, Arrays.asList(1, 2))));
    }

    @Test
    public void testAllNullParameters() throws SqlParseException {
        assertTrue(check("INSERT INTO jdbc (__key, int1) VALUES (?, ?)", Arrays.asList(null, null)));
    }

    @Test
    public void testArrayOfParameter() throws SqlParseException {
        assertFalse(check("INSERT INTO jdbc (__key, l1) VALUES (?, ARRAY[?])", Arrays.asList("k1", 5)));
    }

    @Test
    public void testMultiRow() throws SqlParseException {
        assertFalse(check("INSERT INTO jdbc (__key, int1) VALUES (?, ?), (?, ?)",
                Arrays.asList("k1", 1, "k2", 2)));
    }

    @Test
    public void testLiteralAndArrayOfParameters() throws SqlParseException {
        assertFalse(check("INSERT INTO jdbc (__key, l1) VALUES ('k1', ARRAY[?, ?])", Arrays.asList(1L, 2L)));
    }

    @Test
    public void testNonInsert() throws SqlParseException {
        assertFalse(check("UPDATE jdbc SET int1=? WHERE __key=?", Arrays.asList(1, "k1")));
        assertFalse(check("DELETE FROM jdbc WHERE __key=?", Arrays.asList((Object) "k1")));
    }

    private static boolean check(String sql, List<Object> parameters) throws SqlParseException {
        return isSingleRowParametersInsert(AerospikeQuery.parse(sql, parameters), parameters);
    }
}
