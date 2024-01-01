package com.aerospike.jdbc;

import com.aerospike.jdbc.util.TestRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import static com.aerospike.jdbc.util.TestConfig.TABLE_NAME;
import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static com.aerospike.jdbc.util.TestUtil.sleep;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;

public class IndexQueriesTest extends JdbcBaseTest {

    private final TestRecord testRecord;

    IndexQueriesTest() {
        testRecord = new TestRecord("key1", true, 11100, 1, "bar");
    }

    @BeforeClass
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

    @AfterClass
    public void tearDown() throws SQLException {
        Objects.requireNonNull(connection, "connection is null");
        Statement statement = null;
        String query = format("TRUNCATE TABLE %s", TABLE_NAME);
        try {
            statement = connection.createStatement();
            boolean result = statement.execute(query);
            sleep(100L);
            assertFalse(result);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(statement.getUpdateCount(), 1);
    }

    @Test
    public void testIndexCreateSuccess() throws SQLException {
        Statement statement = null;
        int count;
        String query = format("CREATE INDEX str1_idx ON %s (str1);", TABLE_NAME);
        try {
            statement = connection.createStatement();
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);
    }

    @Test
    public void testIndexCreateMultiColumn() throws SQLException {
        String query = format("CREATE INDEX multi_idx ON %s (str1, int1)", TABLE_NAME);
        final Statement statement = connection.createStatement();
        assertThrows(UnsupportedOperationException.class, () -> statement.executeUpdate(query));
        closeQuietly(statement);
    }

    @Test
    public void testIndexCreateUnsupportedType() throws SQLException {
        String query = format("CREATE INDEX bool1_idx ON %s (bool1)", TABLE_NAME);
        final Statement statement = connection.createStatement();
        assertThrows(UnsupportedOperationException.class, () -> statement.executeUpdate(query));
        closeQuietly(statement);
    }

    @Test
    public void testIndexCreateNonExistentColumn() throws SQLException {
        String query = format("CREATE INDEX ne_idx ON %s (ne)", TABLE_NAME);
        final Statement statement = connection.createStatement();
        assertThrows(IllegalArgumentException.class, () -> statement.executeUpdate(query));
        closeQuietly(statement);
    }

    @Test
    public void testIndexDropSuccess() throws SQLException {
        Statement statement = null;
        int count;
        String query = format("DROP INDEX str1_idx ON %s;", TABLE_NAME);
        try {
            statement = connection.createStatement();
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);
    }
}
