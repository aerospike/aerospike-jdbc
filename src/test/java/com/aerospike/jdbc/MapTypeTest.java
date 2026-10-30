package com.aerospike.jdbc;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
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
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
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
    public void testDefaultMapRepresentationIsUnchanged() throws SQLException {
        insertValue(Collections.singletonMap("foo", Collections.singletonMap("bar", 1L)));

        Object[] result = selectMapColumn(connection);
        assertTrue(result[0] instanceof Map, String.valueOf(result[0]));
        assertEquals(result[1], "{foo={bar=1}}");
    }

    @Test
    public void testMapAsJson() throws SQLException {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("foo", Collections.singletonMap("bar", Collections.singletonMap("baz", 1L)));
        map.put("list", Arrays.asList("a", 2L));
        insertValue(map);

        Object[] result = selectWithMapAsJson();
        String json = (String) result[1];
        assertTrue(json.contains("\"foo\":{\"bar\":{\"baz\":1}}"), json);
        assertTrue(json.contains("\"list\":[\"a\",2]"), json);
        assertEquals(result[0], json);
    }

    @Test
    public void testMapAsJsonWithGeoJsonHllAndNonStringKeys() throws SQLException {
        IAerospikeClient client = ((AerospikeConnection) connection).getClient();
        Key key = new Key(NAMESPACE, TABLE_NAME, TEST_KEY);
        client.operate(null, key, HLLOperation.init(HLLPolicy.Default, "hll_tmp", 4));
        Value.HLLValue hll = client.get(null, key, "hll_tmp").getHLLValue("hll_tmp");

        Map<Object, Object> map = new HashMap<>();
        map.put("geo", Value.getAsGeoJSON("{\"type\": \"Point\", \"coordinates\": [-122.0, 37.5]}"));
        map.put("hll", hll);
        map.put(7L, "long key");
        map.put(new byte[]{1, 2, 3}, "blob key");
        client.put(null, key, new Bin("map_col", Value.get(map)));

        String json = (String) selectWithMapAsJson()[1];
        assertTrue(json.contains("\"geo\":{\"type\":\"Point\",\"coordinates\":[-122.0,37.5]}"), json);
        assertTrue(json.contains("\"hll\":\"" + Base64.getEncoder().encodeToString(hll.getBytes()) + "\""), json);
        assertTrue(json.contains("\"7\":\"long key\""), json);
        assertTrue(json.contains("\"AQID\":\"blob key\""), json);
    }

    @Test
    public void testMapAsJsonWithKeyOrderedBlobKeys() throws SQLException {
        IAerospikeClient client = ((AerospikeConnection) connection).getClient();
        Key key = new Key(NAMESPACE, TABLE_NAME, TEST_KEY);
        Map<Value, Value> map = new HashMap<>();
        map.put(Value.get(new byte[]{4, 5, 6}), Value.get(2L));
        map.put(Value.get(new byte[]{1, 2, 3}), Value.get(1L));
        client.operate(null, key, MapOperation.putItems(new MapPolicy(MapOrder.KEY_ORDERED, 0), "map_col", map));

        assertEquals(selectWithMapAsJson()[1], "{\"AQID\":1,\"BAUG\":2}");
    }

    private Object[] selectWithMapAsJson() throws SQLException {
        String url = String.format("jdbc:aerospike:%s:%d/%s?mapAsJson=true", HOSTNAME, PORT, NAMESPACE);
        try (Connection jsonConnection = DriverManager.getConnection(url)) {
            return selectMapColumn(jsonConnection);
        }
    }

    // returns getObject and getString results for map_col
    private Object[] selectMapColumn(Connection conn) throws SQLException {
        Statement selectStmt = null;
        ResultSet rs = null;
        try {
            String selectQuery = format("SELECT map_col FROM %s WHERE %s='%s'",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, TEST_KEY);
            selectStmt = conn.createStatement();
            rs = selectStmt.executeQuery(selectQuery);

            assertTrue(rs.next());
            Object[] result = {rs.getObject("map_col"), rs.getString("map_col")};
            assertNotNull(result[0]);
            assertEquals(rs.getString(1), result[1]);
            return result;
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
