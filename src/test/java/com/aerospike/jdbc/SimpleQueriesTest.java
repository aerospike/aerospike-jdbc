package com.aerospike.jdbc;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SimpleQueriesTest extends JdbcBaseTest {

    @BeforeMethod
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

    @AfterMethod
    public void tearDown() throws SQLException {
        Objects.requireNonNull(connection, "connection is null");
        Statement statement = null;
        String query = format("DELETE FROM %s", tableName);
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
    public void testSelectQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT * FROM %s LIMIT 10", tableName);
        int total = 0;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                assertAllByColumnLabel(resultSet);
                assertAllByColumnIndex(resultSet);

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
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT *, bin1 FROM %s WHERE %s='key1'", tableName, PRIMARY_KEY_COLUMN_NAME);
        int total = 0;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                assertAllByColumnLabel(resultSet);
                assertAllByColumnIndex(resultSet);

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
        Statement statement = null;
        int count;
        String query = format("INSERT INTO %s (bin1, int1) VALUES (11101, 3), (11102, 4)", tableName);
        try {
            statement = connection.createStatement();
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 2);

        query = format("SELECT %s FROM %s WHERE int1 > 3", PRIMARY_KEY_COLUMN_NAME, tableName);
        int total = 0;
        ResultSet resultSet = null;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                // assert auto-generated primary key
                assertEquals(resultSet.getString(PRIMARY_KEY_COLUMN_NAME).length(), 36);
                assertEquals(resultSet.getString(1).length(), 36);

                total++;
            }
            assertEquals(total, 1);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testUpdateQuery() throws SQLException {
        Statement statement = null;
        int count;
        String query = format("UPDATE %s SET int1=100 WHERE bin1>10000", tableName);
        try {
            statement = connection.createStatement();
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);

        query = format("UPDATE %s SET int1=100 WHERE bin1>20000", tableName);
        try {
            statement = connection.createStatement();
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 0);
    }

    @Test
    public void testSelectCountQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT count(*) FROM %s", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            assertTrue(resultSet.getLong(1) > 0);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectEqualsQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT %s FROM %s WHERE int1 = 1", PRIMARY_KEY_COLUMN_NAME, tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            assertEquals(resultSet.getString(PRIMARY_KEY_COLUMN_NAME), "key1");
            assertEquals(resultSet.getString(1), "key1");
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectNotEqualsQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT %s, int1 FROM %s WHERE int1 <> 2", PRIMARY_KEY_COLUMN_NAME, tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            assertEquals(resultSet.getString(PRIMARY_KEY_COLUMN_NAME), "key1");
            assertEquals(resultSet.getInt("int1"), 1);

            assertEquals(resultSet.getString(1), "key1");
            assertEquals(resultSet.getInt(2), 1);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectOrQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT int1, str1 FROM %s WHERE int1<>1 OR str1 LIKE 'bar' OR bin1 IS NULL",
                tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            assertEquals(resultSet.getInt("int1"), 1);
            assertEquals(resultSet.getString("str1"), "bar");

            assertEquals(resultSet.getInt(1), 1);
            assertEquals(resultSet.getString(2), "bar");
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectAndQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT * FROM %s WHERE int1<=2 AND bin1>=1000 AND str1 IS NOT NULL",
                tableName);
        try {
            statement = connection.createStatement();
            boolean result = statement.execute(query);
            assertTrue(result);

            resultSet = statement.getResultSet();
            assertTrue(resultSet.next());

            assertAllByColumnLabel(resultSet);
            assertAllByColumnIndex(resultSet);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectInQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT * FROM %s WHERE int1 IN (1, 2) AND str1 IS NOT NULL", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            assertAllByColumnLabel(resultSet);
            assertAllByColumnIndex(resultSet);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectBetweenQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = format("SELECT * FROM %s WHERE int1 BETWEEN 1 AND 3", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());

            assertAllByColumnLabel(resultSet);
            assertAllByColumnIndex(resultSet);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }
}
