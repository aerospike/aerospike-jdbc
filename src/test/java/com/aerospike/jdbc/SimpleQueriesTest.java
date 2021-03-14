package com.aerospike.jdbc;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class SimpleQueriesTest extends JdbcBaseTest {

    @BeforeMethod
    public void setUp() throws SQLException {
        Objects.requireNonNull(connection, "connection is null");
        Statement statement = null;
        ResultSet resultSet = null;
        String query = String.format("insert into %s (bin1, int1, str1) values (11100, 1, \"bar\")", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            resultSet.next();
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
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
                assertEquals(resultSet.getInt("bin1"), 11100);
                assertEquals(resultSet.getInt("int1"), 1);
                assertEquals(resultSet.getString("str1"), "bar");

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
        ResultSet resultSet = null;
        String query = String.format("insert into %s (bin1, int1) values (11101, 3)", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertFalse(resultSet.next());
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testUpdateQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = String.format("update %s set int1=100 where bin1>10000", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            assertFalse(resultSet.next());
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    @Test
    public void testSelectCountQuery() throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        String query = String.format("select count(*) from %s", tableName);
        try {
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            resultSet.next();
            assertEquals(resultSet.getObject(1), 1);
        } finally {
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }
}
