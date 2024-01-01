package com.aerospike.jdbc;

import com.aerospike.jdbc.util.TestRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import static org.testng.Assert.assertTrue;

public class PreparedQueriesTest {

    private static final Logger logger = Logger.getLogger(PreparedQueriesTest.class.getName());
    private static Connection connection;

    private final TestRecord testRecord;

    PreparedQueriesTest() {
        testRecord = new TestRecord("key1", true, 11100, 1, "bar",
                new byte[]{1, 2, 3, 4}, null);
    }

    @BeforeClass
    public static void connectionInit() throws Exception {
        logger.info("connectionInit");
        Class.forName("com.aerospike.jdbc.AerospikeDriver").newInstance();
        String url = String.format("jdbc:aerospike:%s:%d/%s?sendKey=true", HOSTNAME, PORT, NAMESPACE);
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
        PreparedStatement statement = null;
        int count;
        String query = testRecord.toPreparedInsertQuery();
        try {
            statement = connection.prepareStatement(query);
            statement.setBytes(1, testRecord.getVal1());
            statement.setNull(2, 0);
            count = statement.executeUpdate();
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);
    }

    @AfterMethod
    public void tearDown() throws SQLException {
        Objects.requireNonNull(connection, "connection is null");
        PreparedStatement statement = null;
        String query = format("delete from %s", TABLE_NAME);
        try {
            statement = connection.prepareStatement(query);
            boolean result = statement.execute();
            assertFalse(result);
        } finally {
            closeQuietly(statement);
        }
        assertTrue(statement.getUpdateCount() > 0);
    }

    @Test
    public void testSelectQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select * from %s limit 10", TABLE_NAME);
        int total = 0;
        try {
            statement = connection.prepareStatement(query);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                testRecord.assertPreparedResultSet(resultSet);

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
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select *, int1 from %s where %s=?", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
        int total = 0;
        try {
            statement = connection.prepareStatement(query);
            statement.setString(1, testRecord.getPrimaryKey());

            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                testRecord.assertPreparedResultSet(resultSet);

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
        PreparedStatement statement = null;
        int count;
        String query = format("insert into %s (int1, int2) values (?, ?), (?, ?)", TABLE_NAME);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 11101);
            statement.setInt(2, 3);
            statement.setInt(3, 11102);
            statement.setInt(4, 4);
            count = statement.executeUpdate();
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 2);
    }

    @Test
    public void testUpdateQuery() throws SQLException {
        PreparedStatement statement = null;
        int count;
        String query = format("update %s set int2=? where int1>?", TABLE_NAME);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 100);
            statement.setInt(2, 10000);
            count = statement.executeUpdate();
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);

        query = format("update %s set int2=? where int1>?", TABLE_NAME);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 100);
            statement.setInt(2, 20000);
            count = statement.executeUpdate();
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 0);
    }

    @Test
    public void testSelectCountQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select count(*) from %s", TABLE_NAME);
        try {
            statement = connection.prepareStatement(query);
            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());

            assertTrue(resultSet.getLong(1) > 0);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectEqualsQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select %s from %s where int2 = ?", PRIMARY_KEY_COLUMN_NAME, TABLE_NAME);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 1);

            resultSet = statement.executeQuery();
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
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select %s, int2 from %s where int2 <> ?", PRIMARY_KEY_COLUMN_NAME, TABLE_NAME);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 2);

            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());

            assertEquals(resultSet.getString(PRIMARY_KEY_COLUMN_NAME), testRecord.getPrimaryKey());
            assertEquals(resultSet.getInt("int2"), testRecord.getInt2());

            assertEquals(resultSet.getString(1), testRecord.getPrimaryKey());
            assertEquals(resultSet.getInt(2), testRecord.getInt2());
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectOrQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String preparedQuery = format("select int2, str1 from %s where int2<>? or str1 like ? or int1 is null",
                TABLE_NAME);
        try {
            statement = connection.prepareStatement(preparedQuery);
            statement.setInt(1, 1);
            statement.setString(2, "bar");

            resultSet = statement.executeQuery();
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
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String preparedQuery = format("select * from %s where int2<=? and int1>=? and str1 is not null",
                TABLE_NAME);
        try {
            statement = connection.prepareStatement(preparedQuery);
            statement.setInt(1, 2);
            statement.setInt(2, 1000);

            boolean result = statement.execute();
            assertTrue(result);

            resultSet = statement.getResultSet();
            assertTrue(resultSet.next());

            testRecord.assertPreparedResultSet(resultSet);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectInQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select * from %s where int2 in (?, ?) and str1 is not null", TABLE_NAME);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 1);
            statement.setInt(2, 2);

            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());

            testRecord.assertPreparedResultSet(resultSet);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectBetweenQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select * from %s where int2 between ? and ? and val1=?", TABLE_NAME);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 1);
            statement.setInt(2, 3);
            statement.setBytes(3, testRecord.getVal1());

            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());

            testRecord.assertPreparedResultSet(resultSet);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }
}
