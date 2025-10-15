package com.aerospike.jdbc;

import com.aerospike.jdbc.util.TestRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

@Ignore
public class TransactionTest {

    private static final Logger logger = Logger.getLogger(TransactionTest.class.getName());
    private static Connection connection;

    private final TestRecord testRecord;
    private final TestRecord testRecord2;

    TransactionTest() {
        testRecord = new TestRecord("key1", true, 11100, 1, "bar");
        testRecord2 = new TestRecord("key2", false, 11101, 2, "foo");
    }

    @BeforeClass
    public static void connectionInit() throws Exception {
        logger.info("connectionInit");
        Class.forName("com.aerospike.jdbc.AerospikeDriver").newInstance();
        String url = String.format(
                "jdbc:aerospike:%s:%d/%s?sendKey=true&user=%s&password=%s&durableDelete=true&refuseScan=false",
                HOSTNAME, PORT, NAMESPACE, "", "");
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
    public void testTransactionCommit() throws SQLException {
        connection.setAutoCommit(false);
        Statement statement = null;
        String query = testRecord2.toInsertQuery();
        try {
            statement = connection.createStatement();
            statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        try {
            statement = connection.createStatement();
            statement.executeUpdate(format("DELETE FROM %s WHERE __key='key1'", TABLE_NAME));
        } finally {
            closeQuietly(statement);
        }
        connection.commit();

        ResultSet resultSet = null;
        query = format("SELECT * FROM %s LIMIT 10", TABLE_NAME);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());
            assertEquals(resultSet.getString(PRIMARY_KEY_COLUMN_NAME), "key2");
            assertFalse(resultSet.next());
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
        connection.setAutoCommit(true);
    }

    @Test
    public void testTransactionRollback() throws SQLException {
        connection.setAutoCommit(false);
        Statement statement = null;
        String query = testRecord2.toInsertQuery();
        try {
            statement = connection.createStatement();
            statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        try {
            statement = connection.createStatement();
            statement.executeUpdate(format("DELETE FROM %s WHERE __key='key1'", TABLE_NAME));
        } finally {
            closeQuietly(statement);
        }
        connection.rollback();

        ResultSet resultSet = null;
        query = format("SELECT * FROM %s LIMIT 10", TABLE_NAME);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());
            assertEquals(resultSet.getString(PRIMARY_KEY_COLUMN_NAME), "key1");
            assertFalse(resultSet.next());
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
        connection.setAutoCommit(true);
    }
}
