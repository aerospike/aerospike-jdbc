package com.aerospike.jdbc;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static com.aerospike.jdbc.util.TestConfig.HOSTNAME;
import static com.aerospike.jdbc.util.TestConfig.NAMESPACE;
import static com.aerospike.jdbc.util.TestConfig.PORT;
import static com.aerospike.jdbc.util.TestConfig.TABLE_NAME;
import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class ArrayTypeTest {

    private static final Logger logger = Logger.getLogger(ArrayTypeTest.class.getName());

    private static final String TEST_KEY = "array_key1";
    private static Connection connection;

    @BeforeClass
    public static void connectionInit() throws Exception {
        logger.info("connectionInit");
        Class.forName("com.aerospike.jdbc.AerospikeDriver").newInstance();
        String url = String.format("jdbc:aerospike:%s:%d/%s?sendKey=true&refuseScan=false",
                HOSTNAME, PORT, NAMESPACE);
        connection = DriverManager.getConnection(url);
        connection.setNetworkTimeout(Executors.newSingleThreadExecutor(), 5000);
    }

    @AfterClass
    public static void connectionClose() throws SQLException {
        logger.info("connectionClose");
        connection.close();
    }

    @BeforeMethod
    public void setUp() throws SQLException {
        // Clean up any existing test data
        tearDown();
    }

    @AfterMethod
    public void tearDown() throws SQLException {
        Objects.requireNonNull(connection, "connection is null");
        Statement statement = null;
        String query = format("DELETE FROM %s WHERE %s='%s'", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, TEST_KEY);
        try {
            statement = connection.createStatement();
            statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
    }

    @Test
    public void testWriteAndReadStringArray() throws SQLException {
        // Write array using PreparedStatement.setArray()
        PreparedStatement insertStmt = null;
        String[] stringArray = {"a", "b", "c"};
        try {
            String insertQuery = format("INSERT INTO %s (%s, list_col) VALUES (?, ?)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
            insertStmt = connection.prepareStatement(insertQuery);
            insertStmt.setString(1, TEST_KEY);

            // Create Array and set it
            Array sqlArray = connection.createArrayOf("VARCHAR", stringArray);
            insertStmt.setArray(2, sqlArray);
            int count = insertStmt.executeUpdate();
            assertEquals(count, 1);
        } finally {
            closeQuietly(insertStmt);
        }

        // Read array using ResultSet.getArray()
        Statement selectStmt = null;
        ResultSet rs = null;
        try {
            String selectQuery = format("SELECT list_col FROM %s WHERE %s='%s'",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, TEST_KEY);
            selectStmt = connection.createStatement();
            rs = selectStmt.executeQuery(selectQuery);

            assertTrue(rs.next());
            Array resultArray = rs.getArray("list_col");
            assertNotNull(resultArray);

            Object[] result = (Object[]) resultArray.getArray();
            assertEquals(result.length, 3);
            assertEquals(result[0], "a");
            assertEquals(result[1], "b");
            assertEquals(result[2], "c");
        } finally {
            closeQuietly(rs);
            closeQuietly(selectStmt);
        }
    }

    @Test
    public void testWriteAndReadIntegerArray() throws SQLException {
        // Write array using PreparedStatement.setObject() with List
        PreparedStatement insertStmt = null;
        List<Integer> intList = Arrays.asList(10, 20, 30, 40);
        try {
            String insertQuery = format("INSERT INTO %s (%s, list_col) VALUES (?, ?)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
            insertStmt = connection.prepareStatement(insertQuery);
            insertStmt.setString(1, TEST_KEY);
            insertStmt.setObject(2, intList);
            int count = insertStmt.executeUpdate();
            assertEquals(count, 1);
        } finally {
            closeQuietly(insertStmt);
        }

        // Read array using ResultSet.getObject()
        Statement selectStmt = null;
        ResultSet rs = null;
        try {
            String selectQuery = format("SELECT list_col FROM %s WHERE %s='%s'",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, TEST_KEY);
            selectStmt = connection.createStatement();
            rs = selectStmt.executeQuery(selectQuery);

            assertTrue(rs.next());
            @SuppressWarnings("unchecked")
            List<Long> result = (List<Long>) rs.getObject("list_col");
            assertNotNull(result);
            assertEquals(result.size(), 4);
            assertEquals(result.get(0), 10L);
            assertEquals(result.get(1), 20L);
            assertEquals(result.get(2), 30L);
            assertEquals(result.get(3), 40L);
        } finally {
            closeQuietly(rs);
            closeQuietly(selectStmt);
        }
    }

    @Test
    public void testWriteAndReadLongArray() throws SQLException {
        // Write array using PreparedStatement.setArray()
        PreparedStatement insertStmt = null;
        Long[] longArray = {100L, 200L, 300L};
        try {
            String insertQuery = format("INSERT INTO %s (%s, list_col) VALUES (?, ?)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
            insertStmt = connection.prepareStatement(insertQuery);
            insertStmt.setString(1, TEST_KEY);

            Array sqlArray = connection.createArrayOf("BIGINT", longArray);
            insertStmt.setArray(2, sqlArray);
            int count = insertStmt.executeUpdate();
            assertEquals(count, 1);
        } finally {
            closeQuietly(insertStmt);
        }

        // Read array using ResultSet.getArray()
        Statement selectStmt = null;
        ResultSet rs = null;
        try {
            String selectQuery = format("SELECT list_col FROM %s WHERE %s='%s'",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, TEST_KEY);
            selectStmt = connection.createStatement();
            rs = selectStmt.executeQuery(selectQuery);

            assertTrue(rs.next());
            Array resultArray = rs.getArray("list_col");
            assertNotNull(resultArray);

            Object[] result = (Object[]) resultArray.getArray();
            assertEquals(result.length, 3);
            assertEquals(result[0], 100L);
            assertEquals(result[1], 200L);
            assertEquals(result[2], 300L);
        } finally {
            closeQuietly(rs);
            closeQuietly(selectStmt);
        }
    }

    @Test
    public void testWriteAndReadMixedTypeArray() throws SQLException {
        // Write array with mixed types (will be converted to common type)
        PreparedStatement insertStmt = null;
        List<Object> mixedList = Arrays.asList("text", 42, 3.14, true);
        try {
            String insertQuery = format("INSERT INTO %s (%s, list_col) VALUES (?, ?)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
            insertStmt = connection.prepareStatement(insertQuery);
            insertStmt.setString(1, TEST_KEY);
            insertStmt.setObject(2, mixedList);
            int count = insertStmt.executeUpdate();
            assertEquals(count, 1);
        } finally {
            closeQuietly(insertStmt);
        }

        // Read array
        Statement selectStmt = null;
        ResultSet rs = null;
        try {
            String selectQuery = format("SELECT list_col FROM %s WHERE %s='%s'",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, TEST_KEY);
            selectStmt = connection.createStatement();
            rs = selectStmt.executeQuery(selectQuery);

            assertTrue(rs.next());
            @SuppressWarnings("unchecked")
            List<Object> result = (List<Object>) rs.getObject("list_col");
            assertNotNull(result);
            assertEquals(result.size(), 4);
        } finally {
            closeQuietly(rs);
            closeQuietly(selectStmt);
        }
    }

    @Test
    public void testWriteArrayViaSqlLiteral() throws SQLException {
        // Write array using SQL array literal syntax
        Statement insertStmt = null;
        try {
            // Note: This depends on SQL parser supporting ARRAY[...] syntax
            String insertQuery = format("INSERT INTO %s (%s, list_col) VALUES ('%s', ARRAY['a', 'b', 'c'])",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, TEST_KEY);
            insertStmt = connection.createStatement();
            int count = insertStmt.executeUpdate(insertQuery);
            assertEquals(count, 1);
        } finally {
            closeQuietly(insertStmt);
        }

        // Verify it was written correctly
        Statement selectStmt = null;
        ResultSet rs = null;
        try {
            String selectQuery = format("SELECT list_col FROM %s WHERE %s='%s'",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, TEST_KEY);
            selectStmt = connection.createStatement();
            rs = selectStmt.executeQuery(selectQuery);

            assertTrue(rs.next());
            Array resultArray = rs.getArray("list_col");
            assertNotNull(resultArray);

            Object[] result = (Object[]) resultArray.getArray();
            assertEquals(result.length, 3);
            assertEquals(result[0], "a");
            assertEquals(result[1], "b");
            assertEquals(result[2], "c");
        } finally {
            closeQuietly(rs);
            closeQuietly(selectStmt);
        }
    }

    @Test
    public void testUpdateArray() throws SQLException {
        // First insert a record
        PreparedStatement insertStmt = null;
        List<String> initialList = Arrays.asList("old1", "old2");
        try {
            String insertQuery = format("INSERT INTO %s (%s, list_col) VALUES (?, ?)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
            insertStmt = connection.prepareStatement(insertQuery);
            insertStmt.setString(1, TEST_KEY);
            insertStmt.setObject(2, initialList);
            insertStmt.executeUpdate();
        } finally {
            closeQuietly(insertStmt);
        }

        // Update the array
        PreparedStatement updateStmt = null;
        List<String> newList = Arrays.asList("new1", "new2", "new3");
        try {
            String updateQuery = format("UPDATE %s SET list_col=? WHERE %s=?",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
            updateStmt = connection.prepareStatement(updateQuery);
            updateStmt.setObject(1, newList);
            updateStmt.setString(2, TEST_KEY);
            int count = updateStmt.executeUpdate();
            assertEquals(count, 1);
        } finally {
            closeQuietly(updateStmt);
        }

        // Verify update
        Statement selectStmt = null;
        ResultSet rs = null;
        try {
            String selectQuery = format("SELECT list_col FROM %s WHERE %s='%s'",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, TEST_KEY);
            selectStmt = connection.createStatement();
            rs = selectStmt.executeQuery(selectQuery);

            assertTrue(rs.next());
            @SuppressWarnings("unchecked")
            List<String> result = (List<String>) rs.getObject("list_col");
            assertNotNull(result);
            assertEquals(result.size(), 3);
            assertEquals(result.get(0), "new1");
            assertEquals(result.get(1), "new2");
            assertEquals(result.get(2), "new3");
        } finally {
            closeQuietly(rs);
            closeQuietly(selectStmt);
        }
    }

    @Test
    public void testArrayGetResultSet() throws SQLException {
        // Write array
        PreparedStatement insertStmt = null;
        Integer[] intArray = {1, 2, 3, 4, 5};
        try {
            String insertQuery = format("INSERT INTO %s (%s, list_col) VALUES (?, ?)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
            insertStmt = connection.prepareStatement(insertQuery);
            insertStmt.setString(1, TEST_KEY);

            Array sqlArray = connection.createArrayOf("INTEGER", intArray);
            insertStmt.setArray(2, sqlArray);
            insertStmt.executeUpdate();
        } finally {
            closeQuietly(insertStmt);
        }

        // Read array and test getResultSet() method
        Statement selectStmt = null;
        ResultSet rs = null;
        ResultSet arrayRs = null;
        try {
            String selectQuery = format("SELECT list_col FROM %s WHERE %s='%s'",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, TEST_KEY);
            selectStmt = connection.createStatement();
            rs = selectStmt.executeQuery(selectQuery);

            assertTrue(rs.next());
            Array resultArray = rs.getArray("list_col");
            assertNotNull(resultArray);

            // Test getResultSet() method
            arrayRs = resultArray.getResultSet();
            assertNotNull(arrayRs);

            int index = 0;
            while (arrayRs.next()) {
                int ordinal = arrayRs.getInt("INDEX");
                int value = arrayRs.getInt("VALUE");
                assertEquals(ordinal, index + 1);
                assertEquals(value, intArray[index]);
                index++;
            }
            assertEquals(index, intArray.length);
        } finally {
            closeQuietly(arrayRs);
            closeQuietly(rs);
            closeQuietly(selectStmt);
        }
    }

    @Test
    public void testNullArray() throws SQLException {
        PreparedStatement insertStmt = null;
        try {
            String insertQuery = format("INSERT INTO %s (%s, list_col, long_col) VALUES (?, ?, ?)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);

            insertStmt = connection.prepareStatement(insertQuery);
            insertStmt.setString(1, TEST_KEY);
            insertStmt.setLong(3, 10L);
            int count = insertStmt.executeUpdate();
            assertEquals(count, 1);
        } finally {
            closeQuietly(insertStmt);
        }

        Statement selectStmt = null;
        ResultSet rs = null;
        try {
            String selectQuery = format("SELECT * FROM %s", TABLE_NAME);
            selectStmt = connection.createStatement();
            rs = selectStmt.executeQuery(selectQuery);

            assertTrue(rs.next());
            assertNull(rs.getArray("list_col"));
            assertNull(rs.getObject("list_col"));

            assertFalse(rs.next());
        } finally {
            closeQuietly(rs);
            closeQuietly(selectStmt);
        }
    }

    @Test
    public void testEmptyArray() throws SQLException {
        PreparedStatement insertStmt = null;
        try {
            String insertQuery = format("INSERT INTO %s (%s, list_col, long_col) VALUES (?, ?, ?)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);

            insertStmt = connection.prepareStatement(insertQuery);
            insertStmt.setString(1, TEST_KEY);
            Array sqlArray = connection.createArrayOf("BIGINT", new Long[0]);
            insertStmt.setArray(2, sqlArray);
            insertStmt.setLong(3, 11L);
            int count = insertStmt.executeUpdate();
            assertEquals(count, 1);
        } finally {
            closeQuietly(insertStmt);
        }

        Statement selectStmt = null;
        ResultSet rs = null;
        try {
            String selectQuery = format("SELECT * FROM %s", TABLE_NAME);
            selectStmt = connection.createStatement();
            rs = selectStmt.executeQuery(selectQuery);

            assertTrue(rs.next());
            Array resultArray = rs.getArray("list_col");
            Object[] result = (Object[]) resultArray.getArray();
            assertEquals(result.length, 0);

            assertFalse(rs.next());
        } finally {
            closeQuietly(rs);
            closeQuietly(selectStmt);
        }
    }
}
