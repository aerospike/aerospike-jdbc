package com.aerospike.jdbc;

import com.aerospike.client.Value;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SimpleQueriesTest extends JdbcBaseTest {

    @BeforeMethod
    public void setUp() throws SQLException {
        Value.UseBoolBin = false;
        Objects.requireNonNull(connection, "connection is null");
        Statement statement = null;
        int count;
        String query = String.format(
                "insert into %s (bin1, int1, str1, bool1) values (11100, 1, \"bar\", true)",
                tableName
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
        ResultSet resultSet = null;
        String query = String.format("delete from %s", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            resultSet.next();
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = String.format("select * from %s limit 10", tableName);
        int total = 0;
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                assertEquals(11100, resultSet.getInt("bin1"));
                assertEquals(1, resultSet.getInt("int1"));
                assertEquals("bar", resultSet.getString("str1"));
                assertEquals(1, resultSet.getInt("bool1"));

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
        String query = String.format("insert into %s (bin1, int1) values (11101, 3), (11102, 4)", tableName);
        try {
            statement = connection.createStatement();
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 2);
    }

    @Test
    public void testUpdateQuery() throws SQLException {
        Statement statement = null;
        int count;
        String query = String.format("update %s set int1=100 where bin1>10000", tableName);
        try {
            statement = connection.createStatement();
            count = statement.executeUpdate(query);
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);

        query = String.format("update %s set int1=100 where bin1>20000", tableName);
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
        String query = String.format("select count(*) from %s", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());
            assertEquals(resultSet.getObject(1), 1);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectEqualsQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = String.format("select __key from %s where int1 = 1", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());
            assertEquals(resultSet.getString(1).length(), 36);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectNotEqualsQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = String.format("select __key, int1 from %s where int1 <> 2", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());
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
        String query = String.format("select int1, str1 from %s where int1<>1 or str1 like 'bar' or bin1 is null", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());
            assertEquals(resultSet.getInt(1), 1);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectAndQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = String.format("select * from %s where int1<=2 and bin1>=1000 and str1 is not null", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());
            assertEquals(resultSet.getInt(2), 11100);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectInQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = String.format("select * from %s where int1 in (1, 2) and str1 is not null", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());
            assertEquals(resultSet.getInt(2), 11100);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectBetweenQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = String.format("select * from %s where int1 between 1 and 3", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next());
            assertEquals(resultSet.getInt(2), 11100);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }
}
