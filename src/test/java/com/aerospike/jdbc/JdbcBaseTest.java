package com.aerospike.jdbc;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class JdbcBaseTest {

    protected static final String namespace = "test";
    protected static final String tableName = "jdbc";

    private static final Logger logger = Logger.getLogger(JdbcBaseTest.class.getName());
    private static final String hostname = "localhost";
    private static final int port = 3000;

    protected static Connection connection;

    @BeforeClass
    public static void connectionInit() throws Exception {
        logger.info("connectionInit");
        Class.forName("com.aerospike.jdbc.AerospikeDriver").newInstance();
        String url = String.format("jdbc:aerospike:%s:%d/%s?sendKey=true", hostname, port, namespace);
        connection = DriverManager.getConnection(url);
        connection.setNetworkTimeout(Executors.newSingleThreadExecutor(), 5000);
    }

    @AfterClass
    public static void connectionClose() throws SQLException {
        logger.info("connectionClose");
        connection.close();
    }

    protected void assertAllByColumnLabel(ResultSet resultSet) throws SQLException {
        assertEquals(resultSet.getString(PRIMARY_KEY_COLUMN_NAME), "key1");
        assertEquals(resultSet.getInt("bin1"), 11100);
        assertTrue(resultSet.getBoolean("bool1"));
        assertEquals(resultSet.getInt("int1"), 1);
        assertEquals(resultSet.getString("str1"), "bar");
    }

    protected void assertAllByColumnIndex(ResultSet resultSet) throws SQLException {
        assertEquals(resultSet.getString(1), "key1");
        assertEquals(resultSet.getInt(2), 11100);
        assertTrue(resultSet.getBoolean(3));
        assertEquals(resultSet.getInt(4), 1);
        assertEquals(resultSet.getString(5), "bar");
    }

    @SuppressWarnings("all")
    protected void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }
}
