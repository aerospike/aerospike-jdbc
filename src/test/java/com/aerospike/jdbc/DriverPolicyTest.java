package com.aerospike.jdbc;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.ResultCode;
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

import static com.aerospike.jdbc.util.TestConfig.HOSTNAME;
import static com.aerospike.jdbc.util.TestConfig.NAMESPACE;
import static com.aerospike.jdbc.util.TestConfig.PORT;
import static com.aerospike.jdbc.util.TestConfig.TABLE_NAME;
import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DriverPolicyTest {

    private static final Logger logger = Logger.getLogger(DriverPolicyTest.class.getName());
    private static Connection connection;
    private static IAerospikeClient client;

    private final TestRecord testRecord;

    DriverPolicyTest() {
        testRecord = new TestRecord("key1", true, 11100, 1, "bar");
    }

    @BeforeClass
    public static void connectionInit() throws Exception {
        logger.info("connectionInit");
        Class.forName("com.aerospike.jdbc.AerospikeDriver").newInstance();
        String url = String.format("jdbc:aerospike:%s:%d/%s?refuseScan=true", HOSTNAME, PORT, NAMESPACE);
        connection = DriverManager.getConnection(url);
        connection.setNetworkTimeout(Executors.newSingleThreadExecutor(), 5000);
        client = ((AerospikeConnection) connection).getClient();
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
    public void testRefuseScan() throws SQLException {
        String query = format("SELECT * FROM %s", TABLE_NAME);
        String setIndexCommand = "set-config:context=namespace;id=%s;set=%s;enable-index=%s";

        // Enable set index
        Info.request(client.getInfoPolicyDefault(), client.getCluster().getRandomNode(),
                format(setIndexCommand, NAMESPACE, TABLE_NAME, "true"));

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        assertTrue(resultSet.next());

        closeQuietly(statement);
        closeQuietly(resultSet);

        // Disable set index
        Info.request(client.getInfoPolicyDefault(), client.getCluster().getRandomNode(),
                format(setIndexCommand, NAMESPACE, TABLE_NAME, "false"));

        statement = connection.createStatement();
        try {
            statement.executeQuery(query);
        } catch (SQLException e) {
            assertTrue(e.getCause() instanceof AerospikeException);
            AerospikeException ae = (AerospikeException) e.getCause();
            assertEquals(ae.getResultCode(), ResultCode.INDEX_NOTFOUND);
            return;
        }

        throw new AssertionError("Expected AerospikeException(INDEX_NOTFOUND) to be thrown");
    }

    @Test
    public void testRefuseScanWithLimit() throws SQLException {
        String query = format("SELECT * FROM %s LIMIT 1", TABLE_NAME);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        assertTrue(resultSet.next());

        closeQuietly(statement);
        closeQuietly(resultSet);
    }
}
