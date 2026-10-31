package com.aerospike.jdbc;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.jdbc.util.TestRecord;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static com.aerospike.jdbc.util.TestConfig.HOSTNAME;
import static com.aerospike.jdbc.util.TestConfig.NAMESPACE;
import static com.aerospike.jdbc.util.TestConfig.PORT;
import static com.aerospike.jdbc.util.TestConfig.TABLE_NAME;
import static com.aerospike.jdbc.util.TestUtil.durableDeleteUrlSuffixIfStrongConsistency;
import static com.aerospike.jdbc.util.TestUtil.isStrongConsistencyNamespace;
import static java.lang.String.format;
import static java.sql.Statement.EXECUTE_FAILED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class BatchQueriesTest extends JdbcBaseTest {

    private static final String INSERT_QUERY = format("INSERT INTO %s (%s, int1, int2, str1) VALUES (?, ?, ?, ?)",
            TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);

    private final TestRecord testRecord = new TestRecord("key1", true, 11100, 1, "bar");

    @BeforeMethod
    public void setUp() throws SQLException {
        Objects.requireNonNull(connection, "connection is null");
        try (Statement statement = connection.createStatement()) {
            assertEquals(statement.executeUpdate(testRecord.toInsertQuery()), 1);
        }
    }

    @AfterMethod
    public void tearDown() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(format("DELETE FROM %s", TABLE_NAME));
        }
    }

    @Test
    public void testPreparedInsertBatchesAreServerBatches() throws SQLException {
        IAerospikeClient client = getClient();
        int rows = 1000;
        int batchSize = 100;
        long batchesBefore = getBatchIndexInitiate(client);

        int executeBatchCalls = 0;
        try (PreparedStatement statement = connection.prepareStatement(INSERT_QUERY)) {
            for (int i = 0; i < rows; i++) {
                addInsertBatch(statement, "load_" + i, i, i % 10);
                if ((i + 1) % batchSize == 0) {
                    int[] counts = statement.executeBatch();
                    executeBatchCalls++;
                    assertEquals(counts.length, batchSize);
                    for (int count : counts) {
                        assertEquals(count, 1);
                    }
                }
            }
        }

        assertEquals(executeBatchCalls, rows / batchSize);
        long batches = getBatchIndexInitiate(client) - batchesBefore;
        if (client.getNodes().length == 1) {
            assertEquals(batches, rows / batchSize);
        } else {
            assertTrue(batches >= rows / batchSize, "batch_index_initiate delta: " + batches);
        }
        for (int i = 0; i < rows; i++) {
            assertTrue(exists("load_" + i), "load_" + i);
        }
    }

    @Test
    public void testPreparedInsertDuplicateKeyFails() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_QUERY)) {
            addInsertBatch(statement, "batch_dup1", 1, 1);
            addInsertBatch(statement, testRecord.getPrimaryKey(), 2, 2);
            addInsertBatch(statement, "batch_dup2", 3, 3);

            BatchUpdateException e = expectThrows(BatchUpdateException.class, statement::executeBatch);
            assertEquals(e.getUpdateCounts(), new int[]{1, EXECUTE_FAILED, 1});
            assertEquals(e.getErrorCode(), ResultCode.KEY_EXISTS_ERROR);
            assertEquals(e.getSQLState(), "23000");

            assertEquals(statement.executeBatch(), new int[0], "batch must be cleared after a failure");
        }
        assertTrue(exists("batch_dup1"));
        assertTrue(exists("batch_dup2"));
        assertEquals(get(testRecord.getPrimaryKey()).getInt("int1"), testRecord.getInt1());
    }

    @Test
    public void testPreparedInsertDuplicateKeySkipped() throws Exception {
        try (Connection skipConnection = openConnection("&batchSkipDuplicateKeys=true");
             PreparedStatement statement = skipConnection.prepareStatement(INSERT_QUERY)) {
            addInsertBatch(statement, "batch_dup1", 1, 1);
            addInsertBatch(statement, testRecord.getPrimaryKey(), 2, 2);
            addInsertBatch(statement, "batch_dup2", 3, 3);

            assertEquals(statement.executeBatch(), new int[]{1, 0, 1});
        }
        assertTrue(exists("batch_dup1"));
        assertTrue(exists("batch_dup2"));
        assertEquals(get(testRecord.getPrimaryKey()).getInt("int1"), testRecord.getInt1());
    }

    @Test
    public void testPreparedInsertDuplicateKeyWithinBatch() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_QUERY)) {
            addInsertBatch(statement, "batch_same", 1, 1);
            addInsertBatch(statement, "batch_same", 2, 2);

            BatchUpdateException e = expectThrows(BatchUpdateException.class, statement::executeBatch);
            assertEquals(e.getUpdateCounts(), new int[]{1, EXECUTE_FAILED});
        }
        assertEquals(get("batch_same").getInt("int1"), 1);
    }

    @Test
    public void testPreparedInsertRejectedBatch() throws SQLException {
        String query = format("INSERT INTO no_such_namespace.%s (%s, int1) VALUES (?, ?)",
                TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            for (int i = 0; i < 2; i++) {
                statement.setString(1, "rejected" + i);
                statement.setInt(2, i);
                statement.addBatch();
            }

            BatchUpdateException e = expectThrows(BatchUpdateException.class, statement::executeBatch);
            assertEquals(e.getUpdateCounts(), new int[]{EXECUTE_FAILED, EXECUTE_FAILED});
            assertEquals(e.getErrorCode(), ResultCode.INVALID_NAMESPACE);
            assertTrue(e.getCause() instanceof AerospikeException);
        }
    }

    @Test
    public void testPreparedInsertMultiRowAndLiteralValues() throws SQLException {
        String multiRowQuery = format("INSERT INTO %s (%s, int1) VALUES (?, ?), (?, ?)",
                TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
        try (PreparedStatement statement = connection.prepareStatement(multiRowQuery)) {
            for (int i = 0; i < 2; i++) {
                statement.setString(1, "multi_a" + i);
                statement.setInt(2, i);
                statement.setString(3, "multi_b" + i);
                statement.setInt(4, i);
                statement.addBatch();
            }
            assertEquals(statement.executeBatch(), new int[]{2, 2});
        }
        assertTrue(exists("multi_a1"));
        assertTrue(exists("multi_b1"));

        String literalQuery = format("INSERT INTO %s (%s, int1, str1) VALUES (?, ?, 'literal')",
                TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
        try (PreparedStatement statement = connection.prepareStatement(literalQuery)) {
            for (int i = 0; i < 2; i++) {
                statement.setString(1, "literal" + i);
                statement.setInt(2, i);
                statement.addBatch();
            }
            assertEquals(statement.executeBatch(), new int[]{1, 1});
        }
        assertEquals(get("literal1").getString("str1"), "literal");
    }

    @Test
    public void testPreparedUpdateBatch() throws SQLException {
        String query = format("UPDATE %s SET int2=? WHERE %s=?", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, 100);
            statement.setString(2, testRecord.getPrimaryKey());
            statement.addBatch();
            statement.setInt(1, 200);
            statement.setString(2, "no_such_key");
            statement.addBatch();

            assertEquals(statement.executeBatch(), new int[]{1, 0});
        }
        assertEquals(get(testRecord.getPrimaryKey()).getInt("int2"), 100);
        assertFalse(exists("no_such_key"));
    }

    @Test
    public void testPreparedDeleteBatch() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_QUERY)) {
            addInsertBatch(statement, "del1", 1, 1);
            addInsertBatch(statement, "del2", 2, 2);
            assertEquals(statement.executeBatch(), new int[]{1, 1});
        }

        String query = format("DELETE FROM %s WHERE %s=?", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            for (String key : new String[]{"del1", "del2", "no_such_key"}) {
                statement.setString(1, key);
                statement.addBatch();
            }
            assertEquals(statement.executeBatch(), new int[]{1, 1, 0});
        }
        assertFalse(exists("del1"));
        assertFalse(exists("del2"));
        assertTrue(exists(testRecord.getPrimaryKey()));
    }

    @Test
    public void testStatementBatch() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.addBatch(format("INSERT INTO %s (%s, int1, int2) VALUES ('s1', 1, 1)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME));
            // Same key as the previous command: must be applied after the INSERT.
            statement.addBatch(format("UPDATE %s SET int2=5 WHERE %s='s1'", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME));
            statement.addBatch(format("INSERT INTO %s (%s, int1) VALUES ('s2', 2), ('s3', 3)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME));
            statement.addBatch(format("DELETE FROM %s WHERE %s='s3'", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME));
            statement.addBatch(format("UPDATE %s SET int2=7 WHERE %s='no_such_key'",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME));
            statement.addBatch(format("DELETE FROM %s WHERE %s='no_such_key'", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME));
            // Scan-based UPDATE and DELETE, executed on their own.
            statement.addBatch(format("UPDATE %s SET str1='scanned' WHERE int1 > 10000", TABLE_NAME));
            statement.addBatch(format("DELETE FROM %s WHERE int1 = 2", TABLE_NAME));

            assertEquals(statement.executeBatch(), new int[]{1, 1, 2, 1, 0, 0, 1, 1});
            assertEquals(statement.getUpdateCount(), 7);
            assertEquals(statement.executeBatch(), new int[0]);
        }
        assertEquals(get("s1").getInt("int2"), 5);
        assertFalse(exists("s2"));
        assertFalse(exists("s3"));
        assertFalse(exists("no_such_key"));
        assertEquals(get(testRecord.getPrimaryKey()).getString("str1"), "scanned");
    }

    @Test
    public void testStatementBatchWithFailures() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.addBatch(format("INSERT INTO %s (%s, int1) VALUES ('%s', 1)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, testRecord.getPrimaryKey()));
            statement.addBatch(format("INSERT INTO %s (%s, int1) VALUES ('f1', 1)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME));
            statement.addBatch(format("CREATE INDEX no_such_idx ON %s (no_such_column)", TABLE_NAME));
            statement.addBatch(format("UPDATE %s SET int2=3 WHERE %s='f1'", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME));

            BatchUpdateException e = expectThrows(BatchUpdateException.class, statement::executeBatch);
            assertEquals(e.getUpdateCounts(), new int[]{EXECUTE_FAILED, 1, EXECUTE_FAILED, 1});
            assertEquals(e.getErrorCode(), ResultCode.KEY_EXISTS_ERROR);
            assertEquals(e.getSQLState(), "23000");
            assertTrue(e.getMessage().contains("2 of 4 commands failed"), e.getMessage());
        }
        assertEquals(get("f1").getInt("int2"), 3);
        assertEquals(get(testRecord.getPrimaryKey()).getInt("int1"), testRecord.getInt1());
    }

    @Test
    public void testExecuteLargeBatch() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.addBatch(format("INSERT INTO %s (%s, int1) VALUES ('large1', 1), ('large2', 2)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME));
            statement.addBatch(format("UPDATE %s SET int2=4 WHERE %s='large1'", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME));
            assertEquals(statement.executeLargeBatch(), new long[]{2, 1});
        }

        try (PreparedStatement statement = connection.prepareStatement(INSERT_QUERY)) {
            addInsertBatch(statement, "large3", 3, 3);
            addInsertBatch(statement, "large1", 4, 4);

            BatchUpdateException e = expectThrows(BatchUpdateException.class, statement::executeLargeBatch);
            assertEquals(e.getLargeUpdateCounts(), new long[]{1, EXECUTE_FAILED});
            assertEquals(e.getUpdateCounts(), new int[]{1, EXECUTE_FAILED});
        }
        assertEquals(get("large1").getInt("int2"), 4);
        assertTrue(exists("large3"));
    }

    @Test
    public void testStatementBatchRejectsQueries() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            assertThrows(SQLException.class, () -> statement.addBatch(format("SELECT * FROM %s", TABLE_NAME)));
            assertThrows(SQLException.class, () -> statement.addBatch(format("EXPLAIN SELECT * FROM %s",
                    TABLE_NAME)));
            assertEquals(statement.executeBatch(), new int[0]);
        }
    }

    @Test
    public void testStatementClearBatch() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.addBatch(format("INSERT INTO %s (%s, int1) VALUES ('cleared', 1)",
                    TABLE_NAME, PRIMARY_KEY_COLUMN_NAME));
            statement.clearBatch();
            assertEquals(statement.executeBatch(), new int[0]);
        }
        assertFalse(exists("cleared"));
    }

    @Test
    public void testPreparedStatementRejectsSqlBatch() throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_QUERY)) {
            assertThrows(SQLException.class, () -> statement.addBatch(format(
                    "INSERT INTO %s (%s, int1) VALUES ('x', 1)", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME)));
        }
    }

    @Test
    public void testStatementBatchTransactionRollback() throws Exception {
        if (!isStrongConsistencyNamespace(HOSTNAME, PORT, NAMESPACE)) {
            throw new SkipException("Transactions require a strong-consistency namespace");
        }
        try (Connection txnConnection = openConnection("")) {
            txnConnection.setAutoCommit(false);
            try (Statement statement = txnConnection.createStatement()) {
                statement.addBatch(format("INSERT INTO %s (%s, int1) VALUES ('txn1', 1)",
                        TABLE_NAME, PRIMARY_KEY_COLUMN_NAME));
                statement.addBatch(format("UPDATE %s SET int2=9 WHERE %s='%s'",
                        TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, testRecord.getPrimaryKey()));
                assertEquals(statement.executeBatch(), new int[]{1, 1});
            }
            txnConnection.rollback();
            txnConnection.setAutoCommit(true);
        }
        assertFalse(exists("txn1"));
        assertEquals(get(testRecord.getPrimaryKey()).getInt("int2"), testRecord.getInt2());
    }

    @Test
    public void testPreparedInsertBatchArrayOfParameter() throws SQLException {
        String query = format("INSERT INTO %s (%s, l1) VALUES (?, ARRAY[?])", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, "arr1");
            statement.setInt(2, 5);
            statement.addBatch();
            statement.setString(1, "arr2");
            statement.setInt(2, 6);
            statement.addBatch();
            assertEquals(statement.executeBatch(), new int[]{1, 1});
        }
        assertEquals(get("arr1").getList("l1"), Collections.singletonList(5L));
        assertEquals(get("arr2").getList("l1"), Collections.singletonList(6L));
    }

    @Test
    public void testPreparedInsertBatchLiteralKeyArrayOfParameters() throws SQLException {
        String query = format("INSERT INTO %s (%s, l1) VALUES ('k1', ARRAY[?, ?])", TABLE_NAME,
                PRIMARY_KEY_COLUMN_NAME);
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, 1);
            statement.setInt(2, 2);
            statement.addBatch();
            assertEquals(statement.executeBatch(), new int[]{1});
        }
        assertEquals(get("k1").getList("l1"), Arrays.asList(1L, 2L));
    }

    private static void addInsertBatch(PreparedStatement statement, String key, int int1, int int2)
            throws SQLException {
        statement.setString(1, key);
        statement.setInt(2, int1);
        statement.setInt(3, int2);
        statement.setString(4, "value_" + key);
        statement.addBatch();
    }

    private static Connection openConnection(String extraParams) throws SQLException {
        String url = format("jdbc:aerospike:%s:%d/%s?sendKey=true&refuseScan=false%s%s",
                HOSTNAME, PORT, NAMESPACE, durableDeleteUrlSuffixIfStrongConsistency(HOSTNAME, PORT, NAMESPACE),
                extraParams);
        return DriverManager.getConnection(url);
    }

    private static IAerospikeClient getClient() {
        return ((AerospikeConnection) connection).getClient();
    }

    private static boolean exists(String key) {
        return getClient().exists(null, new Key(NAMESPACE, TABLE_NAME, key));
    }

    private static Record get(String key) {
        Record rec = getClient().get(null, new Key(NAMESPACE, TABLE_NAME, key));
        assertNotNull(rec, key);
        return rec;
    }

    private static long getBatchIndexInitiate(IAerospikeClient client) {
        long total = 0;
        for (Node node : client.getNodes()) {
            String statistics = Info.request(client.getInfoPolicyDefault(), node, "statistics");
            for (String stat : statistics.split(";")) {
                if (stat.startsWith("batch_index_initiate=")) {
                    total += Long.parseLong(stat.substring("batch_index_initiate=".length()));
                }
            }
        }
        return total;
    }
}
