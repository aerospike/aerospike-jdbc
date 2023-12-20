package com.aerospike.jdbc;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class PreparedQueriesTest extends JdbcBaseTest {

    private final byte[] blobValue = new byte[]{1, 2, 3, 4};

    @BeforeMethod
    public void setUp() throws SQLException {
        Objects.requireNonNull(connection, "connection is null");
        PreparedStatement statement = null;
        int count;
        String query = format(
                "insert into %s (%s, bin1, int1, str1, bool1, val1, val2) values "
                        + "(\"key1\", 11100, 1, \"bar\", true, ?, ?)",
                tableName,
                PRIMARY_KEY_COLUMN_NAME
        );
        try {
            statement = connection.prepareStatement(query);
            statement.setBytes(1, blobValue);
            statement.setNull(2, 0);
            count = statement.executeUpdate();
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);
    }

    @AfterMethod
    public void tearDown() throws SQLException {
        Objects.requireNonNull(connection, "connection is null");
        PreparedStatement statement = null;
        String query = format("delete from %s", tableName);
        try {
            statement = connection.prepareStatement(query);
            boolean result = statement.execute();
            assertFalse(result);
        } finally {
            closeQuietly(statement);
        }
        assertTrue(statement.getUpdateCount() > 0);
    }

    @Test
    public void testSelectQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select * from %s limit 10", tableName);
        int total = 0;
        try {
            statement = connection.prepareStatement(query);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                assertAllByColumnLabel(resultSet);
                assertAllByColumnIndex(resultSet);
                assertBlobValue(resultSet);

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
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select *, bin1 from %s where %s=?", tableName, PRIMARY_KEY_COLUMN_NAME);
        int total = 0;
        try {
            statement = connection.prepareStatement(query);
            statement.setString(1, "key1");

            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                assertAllByColumnLabel(resultSet);
                assertAllByColumnIndex(resultSet);
                assertBlobValue(resultSet);

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
        PreparedStatement statement = null;
        int count;
        String query = format("insert into %s (bin1, int1) values (?, ?), (?, ?)", tableName);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 11101);
            statement.setInt(2, 3);
            statement.setInt(3, 11102);
            statement.setInt(4, 4);
            count = statement.executeUpdate();
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 2);
    }

    @Test
    public void testUpdateQuery() throws SQLException {
        PreparedStatement statement = null;
        int count;
        String query = format("update %s set int1=? where bin1>?", tableName);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 100);
            statement.setInt(2, 10000);
            count = statement.executeUpdate();
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);

        query = format("update %s set int1=? where bin1>?", tableName);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 100);
            statement.setInt(2, 20000);
            count = statement.executeUpdate();
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 0);
    }

    @Test
    public void testSelectCountQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select count(*) from %s", tableName);
        try {
            statement = connection.prepareStatement(query);
            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());

            assertTrue(resultSet.getLong(1) > 0);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectEqualsQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select %s from %s where int1 = ?", PRIMARY_KEY_COLUMN_NAME, tableName);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 1);

            resultSet = statement.executeQuery();
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
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select %s, int1 from %s where int1 <> ?", PRIMARY_KEY_COLUMN_NAME, tableName);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 2);

            resultSet = statement.executeQuery();
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
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String preparedQuery = format("select int1, str1 from %s where int1<>? or str1 like ? or bin1 is null",
                tableName);
        try {
            statement = connection.prepareStatement(preparedQuery);
            statement.setInt(1, 1);
            statement.setString(2, "bar");

            resultSet = statement.executeQuery();
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
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String preparedQuery = format("select * from %s where int1<=? and bin1>=? and str1 is not null",
                tableName);
        try {
            statement = connection.prepareStatement(preparedQuery);
            statement.setInt(1, 2);
            statement.setInt(2, 1000);

            boolean result = statement.execute();
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
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select * from %s where int1 in (?, ?) and str1 is not null", tableName);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 1);
            statement.setInt(2, 2);

            resultSet = statement.executeQuery();
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
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("select * from %s where int1 between ? and ? and val1=?", tableName);
        try {
            statement = connection.prepareStatement(query);
            statement.setInt(1, 1);
            statement.setInt(2, 3);
            statement.setBytes(3, blobValue);

            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());

            assertAllByColumnLabel(resultSet);
            assertAllByColumnIndex(resultSet);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    private void assertBlobValue(ResultSet resultSet) throws SQLException {
        assertEquals(resultSet.getBytes("val1"), blobValue);
        assertEquals(resultSet.getBytes(6), blobValue);
    }
}
