package com.aerospike.jdbc;

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
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static com.aerospike.jdbc.util.TestConfig.HOSTNAME;
import static com.aerospike.jdbc.util.TestConfig.NAMESPACE;
import static com.aerospike.jdbc.util.TestConfig.PORT;
import static com.aerospike.jdbc.util.TestConfig.TABLE_NAME;
import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static com.aerospike.jdbc.util.TestUtil.durableDeleteUrlSuffixIfStrongConsistency;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MapTypeTest {

    private static final Logger logger = Logger.getLogger(MapTypeTest.class.getName());

    private static final String TEST_KEY = "map_key1";
    private static Connection connection;

    @BeforeClass
    public static void connectionInit() throws Exception {
        logger.info("connectionInit");
        Class.forName("com.aerospike.jdbc.AerospikeDriver").newInstance();
        String durableSuffix = durableDeleteUrlSuffixIfStrongConsistency(HOSTNAME, PORT, NAMESPACE);
        if (!durableSuffix.isEmpty()) {
            logger.info("namespace " + NAMESPACE + " has strong-consistency; enabling durableDelete on JDBC URL");
        }
        String url = String.format(
                "jdbc:aerospike:%s:%d/%s?sendKey=true&refuseScan=false%s",
                HOSTNAME, PORT, NAMESPACE, durableSuffix);
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
    public void testGetStringReturnsJsonForMap() throws SQLException {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("foo", Collections.singletonMap("bar", Collections.singletonMap("baz", 1L)));
        map.put("list", Arrays.asList("a", 2L));
        insertValue(map);

        Statement selectStmt = null;
        ResultSet rs = null;
        try {
            String selectQuery = format("SELECT map_col FROM %s WHERE %s='%s'",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, TEST_KEY);
            selectStmt = connection.createStatement();
            rs = selectStmt.executeQuery(selectQuery);

            assertTrue(rs.next());
            String json = rs.getString("map_col");
            assertNotNull(json);
            assertTrue(json.contains("\"foo\":{\"bar\":{\"baz\":1}}"), json);
            assertTrue(json.contains("\"list\":[\"a\",2]"), json);
            assertEquals(rs.getString(1), json);

            // getObject still returns the native map
            assertTrue(rs.getObject("map_col") instanceof Map);
        } finally {
            closeQuietly(rs);
            closeQuietly(selectStmt);
        }
    }

    private void insertValue(Object value) throws SQLException {
        PreparedStatement insertStmt = null;
        try {
            String insertQuery = format("INSERT INTO %s (%s, map_col) VALUES (?, ?)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
            insertStmt = connection.prepareStatement(insertQuery);
            insertStmt.setString(1, TEST_KEY);
            insertStmt.setObject(2, value);
            int count = insertStmt.executeUpdate();
            assertEquals(count, 1);
        } finally {
            closeQuietly(insertStmt);
        }
    }
}
