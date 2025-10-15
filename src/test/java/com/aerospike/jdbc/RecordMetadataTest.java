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
import static com.aerospike.jdbc.util.Constants.METADATA_GEN_COLUMN_NAME;
import static com.aerospike.jdbc.util.Constants.METADATA_TTL_COLUMN_NAME;
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
import static org.testng.Assert.assertTrue;

public class RecordMetadataTest {

    private static final Logger logger = Logger.getLogger(RecordMetadataTest.class.getName());
    private static Connection connection;

    private final TestRecord testRecord;

    RecordMetadataTest() {
        testRecord = new TestRecord("key1", true, 11100, 1, "bar");
    }

    @BeforeClass
    public static void connectionInit() throws Exception {
        logger.info("connectionInit");
        Class.forName("com.aerospike.jdbc.AerospikeDriver").newInstance();
        String url = String.format("jdbc:aerospike:%s:%d/%s?sendKey=true&showRecordMetadata=true&refuseScan=false",
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
    public void testSelectAllColumns() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT * FROM %s LIMIT 10", TABLE_NAME);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());
            assertEquals(resultSet.getString(METADATA_DIGEST_COLUMN_NAME), "212ddf97ff3fe0f6dec5e1626d92a635a55171c2");
            assertEquals(resultSet.getInt(METADATA_GEN_COLUMN_NAME), 1);
            assertFalse(resultSet.next());
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectMetadataColumns() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT %s, %s, int1 FROM %s WHERE %s='key1'", METADATA_GEN_COLUMN_NAME,
                METADATA_TTL_COLUMN_NAME, TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());
            assertNull(resultSet.getObject(METADATA_DIGEST_COLUMN_NAME));
            assertEquals(resultSet.getInt(METADATA_GEN_COLUMN_NAME), 1);
            assertEquals(resultSet.getInt("int1"), 11100);
            assertFalse(resultSet.next());
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }
}
