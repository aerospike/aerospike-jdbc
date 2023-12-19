package com.aerospike.jdbc;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;

public class IndexQueriesTest extends JdbcBaseTest {

    @BeforeClass
    public void setUp() throws SQLException {
        Objects.requireNonNull(connection, "connection is null");
        Statement statement = null;
        int count;
        String query = format(
                "INSERT INTO %s (%s, bin1, int1, str1, bool1) VALUES (\"key1\", 11100, 1, \"bar\", true)",
                tableName,
                PRIMARY_KEY_COLUMN_NAME
        );
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
        String query = format("TRUNCATE TABLE %s", tableName);
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
        String query = format("CREATE INDEX str1_idx ON %s (str1);", tableName);
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
        String query = format("CREATE INDEX multi_idx ON %s (str1, int1)", tableName);
        final Statement statement = connection.createStatement();
        assertThrows(UnsupportedOperationException.class, () -> statement.executeUpdate(query));
        closeQuietly(statement);
    }

    @Test
    public void testIndexCreateUnsupportedType() throws SQLException {
        String query = format("CREATE INDEX bool1_idx ON %s (bool1)", tableName);
        final Statement statement = connection.createStatement();
        assertThrows(UnsupportedOperationException.class, () -> statement.executeUpdate(query));
        closeQuietly(statement);
    }

    @Test
    public void testIndexCreateNonExistentColumn() throws SQLException {
        String query = format("CREATE INDEX ne_idx ON %s (ne)", tableName);
        final Statement statement = connection.createStatement();
        assertThrows(IllegalArgumentException.class, () -> statement.executeUpdate(query));
        closeQuietly(statement);
    }

    @Test
    public void testIndexDropSuccess() throws SQLException {
        Statement statement = null;
        int count;
        String query = format("DROP INDEX str1_idx ON %s;", tableName);
        try {
            statement = connection.createStatement();
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);
    }
}
