package com.aerospike.jdbc;

import com.aerospike.jdbc.util.TestRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static com.aerospike.jdbc.util.Constants.METADATA_DIGEST_COLUMN_NAME;
import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static com.aerospike.jdbc.util.TestConfig.HOSTNAME;
import static com.aerospike.jdbc.util.TestConfig.NAMESPACE;
import static com.aerospike.jdbc.util.TestConfig.PORT;
import static com.aerospike.jdbc.util.TestConfig.TABLE_NAME;
import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class SimpleQueriesTest {

    private static final Logger logger = Logger.getLogger(SimpleQueriesTest.class.getName());
    private static Connection connection;

    private final TestRecord testRecord;

    SimpleQueriesTest() {
        testRecord = new TestRecord("key1", true, 11100, 1, "bar");
    }

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
        Objects.requireNonNull(connection, "connection is null");
        Statement statement = null;
        int count;
        String query = testRecord.toInsertQuery();
        try {
            statement = connection.createStatement();
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);
    }

    @AfterMethod
    public void tearDown() throws SQLException {
        Objects.requireNonNull(connection, "connection is null");
        Statement statement = null;
        String query = format("DELETE FROM %s", TABLE_NAME);
        try {
            statement = connection.createStatement();
            boolean result = statement.execute(query);
            assertFalse(result);
        } finally {
            closeQuietly(statement);
        }
        assertTrue(statement.getUpdateCount() > 0);
    }

    @Test
    public void testSelectQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT * FROM %s LIMIT 10", TABLE_NAME);
        int total = 0;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                assertNull(resultSet.getObject(METADATA_DIGEST_COLUMN_NAME));
                testRecord.assertResultSet(resultSet);

                total++;
            }
            assertEquals(total, 1);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectByPrimaryKeyQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT *, int1 FROM %s WHERE %s='key1'", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
        int total = 0;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                testRecord.assertResultSet(resultSet);

                total++;
            }
            assertEquals(total, 1);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testInsertQuery() throws SQLException {
        Statement statement = null;
        int count;
        String query = format("INSERT INTO %s (int1, int2) VALUES (11101, 3), (11102, 4)", TABLE_NAME);
        try {
            statement = connection.createStatement();
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 2);

        query = format("SELECT %s FROM %s WHERE int2 > 3", PRIMARY_KEY_COLUMN_NAME, TABLE_NAME);
        int total = 0;
        ResultSet resultSet = null;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                // assert auto-generated primary key
                assertEquals(resultSet.getString(PRIMARY_KEY_COLUMN_NAME).length(), 36);
                assertEquals(resultSet.getString(1).length(), 36);

                total++;
            }
            assertEquals(total, 1);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testUpdateQuery() throws SQLException {
        Statement statement = null;
        int count;
        String query = format("UPDATE %s SET int2=100 WHERE int1>10000", TABLE_NAME);
        try {
            statement = connection.createStatement();
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);

        query = format("UPDATE %s SET int2=100 WHERE int1>20000", TABLE_NAME);
        try {
            statement = connection.createStatement();
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 0);
    }

    @Test
    public void testSelectCountQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT count(*) FROM %s", TABLE_NAME);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            assertTrue(resultSet.getLong(1) > 0);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectEqualsQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT %s FROM %s WHERE int2 = 1", PRIMARY_KEY_COLUMN_NAME, TABLE_NAME);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            assertEquals(resultSet.getString(PRIMARY_KEY_COLUMN_NAME), testRecord.getPrimaryKey());
            assertEquals(resultSet.getString(1), testRecord.getPrimaryKey());
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectNotEqualsQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT ne1, int2, ne2, bool1, ne3, %s, ne4 FROM %s WHERE int2 <> 2",
                PRIMARY_KEY_COLUMN_NAME, TABLE_NAME);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            assertEquals(resultSet.getString(PRIMARY_KEY_COLUMN_NAME), testRecord.getPrimaryKey());
            assertEquals(resultSet.getInt("int2"), testRecord.getInt2());
            assertEquals(resultSet.getBoolean("bool1"), testRecord.getBool1());

            assertEquals(resultSet.getInt(1), testRecord.getInt2());
            assertEquals(resultSet.getBoolean(2), testRecord.getBool1());
            assertEquals(resultSet.getString(3), testRecord.getPrimaryKey());
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectOrQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT int2, str1 FROM %s WHERE %s='key1' OR %s='key2'",
                TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            assertEquals(resultSet.getInt("int2"), testRecord.getInt2());
            assertEquals(resultSet.getString("str1"), testRecord.getStr1());

            assertEquals(resultSet.getInt(1), testRecord.getInt2());
            assertEquals(resultSet.getString(2), testRecord.getStr1());
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectAndQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT * FROM %s WHERE int2<=2 AND int1>=1000 AND str1 IS NOT NULL",
                TABLE_NAME);
        try {
            statement = connection.createStatement();
            boolean result = statement.execute(query);
            assertTrue(result);

            resultSet = statement.getResultSet();
            assertTrue(resultSet.next());

            testRecord.assertResultSet(resultSet);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectInQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT * FROM %s WHERE int2 IN (1, 2) AND str1 IS NOT NULL", TABLE_NAME);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            testRecord.assertResultSet(resultSet);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectLowerBoundaryBetweenQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT * FROM %s WHERE int2 BETWEEN 1 AND 3", TABLE_NAME);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            testRecord.assertResultSet(resultSet);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectUpperBoundaryBetweenQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT * FROM %s WHERE int2 BETWEEN 0 AND 1", TABLE_NAME);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            testRecord.assertResultSet(resultSet);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectInQueryWithStringValues() throws SQLException {
        Statement statement = null;
        // Insert additional records with different string values
        String insertQuery1 = format("INSERT INTO %s (%s, bool1, int1, int2, str1) VALUES ('key2', false, 11101, 2, 'foo')", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
        String insertQuery2 = format("INSERT INTO %s (%s, bool1, int1, int2, str1) VALUES ('key3', true, 11102, 3, 'baz')", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
        String insertQuery3 = format("INSERT INTO %s (%s, bool1, int1, int2, str1) VALUES ('key4', false, 11103, 4, 'qux')", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);

        try {
            statement = connection.createStatement();
            statement.executeUpdate(insertQuery1);
            statement.executeUpdate(insertQuery2);
            statement.executeUpdate(insertQuery3);
        } finally {
            closeQuietly(statement);
        }

        ResultSet resultSet = null;
        String query = format("SELECT * FROM %s WHERE %s IN ('key2', 'key3', 'foo')", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
        int count = 0;
        try {
            statement = connection.createStatement();
            //SELECT * FROM jdbc WHERE __key IN ('key2', 'key3', 'foo')
            resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                String str1 = resultSet.getString(PRIMARY_KEY_COLUMN_NAME);
                assertTrue(str1.equals("key2") || str1.equals("key3"),
                        "__key should be one of: key2, key3, but was: " + str1);
                count++;
            }
            assertEquals(count, 2, "Should return 2 records matching IN clause");
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }

        resultSet = null;
        query = format("SELECT * FROM %s WHERE %s='key2' OR %s='key4' OR %s='foo'",
                TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME);
        count = 0;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                String str1 = resultSet.getString(PRIMARY_KEY_COLUMN_NAME);
                assertTrue(str1.equals("key2") || str1.equals("key4"),
                        "__key should be one of: key2, key3, but was: " + str1);
                count++;
            }
            assertEquals(count, 2, "Should return 2 records matching OR clause");
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testStatementClosed() {
        String query = format("SELECT count(*) FROM %s", TABLE_NAME);
        assertThrows(SQLException.class, () -> {
            Statement statement = connection.createStatement();
            statement.close();
            statement.executeQuery(query);
        });
    }
}
