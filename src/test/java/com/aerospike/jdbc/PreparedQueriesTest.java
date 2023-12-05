package com.aerospike.jdbc;

import com.aerospike.client.Value;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PreparedQueriesTest extends JdbcBaseTest {

    @BeforeMethod
    public void setUp() throws SQLException {
        Value.UseBoolBin = false;
        Objects.requireNonNull(connection, "connection is null");
        PreparedStatement statement = null;
        int count;
        String query = String.format(
                "insert into %s (%s, bin1, int1, str1, bool1) values (\"key1\", 11100, 1, \"bar\", true)",
                tableName,
                PRIMARY_KEY_COLUMN_NAME
        );
        try {
            statement = connection.prepareStatement(query);
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
        ResultSet resultSet = null;
        String query = String.format("delete from %s", tableName);
        try {
            statement = connection.prepareStatement(query);
            resultSet = statement.executeQuery();
            resultSet.next();
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = String.format("select * from %s limit 10", tableName);
        int total = 0;
        try {
            statement = connection.prepareStatement(query);
            resultSet = statement.executeQuery();
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
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = String.format("select *, bin1 from %s where %s='key1'", tableName, PRIMARY_KEY_COLUMN_NAME);
        int total = 0;
        try {
            statement = connection.prepareStatement(query);
            resultSet = statement.executeQuery();
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
        PreparedStatement statement = null;
        int count;
        String query = String.format("insert into %s (bin1, int1) values (11101, 3), (11102, 4)", tableName);
        try {
            statement = connection.prepareStatement(query);
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
        String query = String.format("update %s set int1=100 where bin1>10000", tableName);
        try {
            statement = connection.prepareStatement(query);
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);

        query = String.format("update %s set int1=100 where bin1>20000", tableName);
        try {
            statement = connection.prepareStatement(query);
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 0);
    }

    @Test
    public void testSelectCountQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = String.format("select count(*) from %s", tableName);
        try {
            statement = connection.prepareStatement(query);
            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());

            assertEquals(resultSet.getObject(1), 1);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectEqualsQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = String.format("select %s from %s where int1 = 1", PRIMARY_KEY_COLUMN_NAME, tableName);
        try {
            statement = connection.prepareStatement(query);
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
        String query = String.format("select %s, int1 from %s where int1 <> 2", PRIMARY_KEY_COLUMN_NAME, tableName);
        try {
            statement = connection.prepareStatement(query);
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
        String query = String.format("select int1, str1 from %s where int1<>1 or str1 like 'bar' or bin1 is null",
                tableName);
        try {
            statement = connection.prepareStatement(query);
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
        String query = String.format("select * from %s where int1<=2 and bin1>=1000 and str1 is not null",
                tableName);
        try {
            statement = connection.prepareStatement(query);
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
    public void testSelectInQuery() throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = String.format("select * from %s where int1 in (1, 2) and str1 is not null", tableName);
        try {
            statement = connection.prepareStatement(query);
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
        String query = String.format("select * from %s where int1 between 1 and 3", tableName);
        try {
            statement = connection.prepareStatement(query);
            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());

            assertAllByColumnLabel(resultSet);
            assertAllByColumnIndex(resultSet);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }
}
