package com.aerospike.jdbc;

import com.aerospike.client.Value;
import com.aerospike.jdbc.util.TestUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DatabaseMetadataTest extends JdbcBaseTest {

    @BeforeClass
    public void setUp() throws SQLException {
        Value.UseBoolBin = false;
        Objects.requireNonNull(connection, "connection is null");
        PreparedStatement statement = null;
        int count;
        String query = format("insert into %s (bin1, int1, str1, bool1) values (11100, 1, \"bar\", true)",
                tableName);
        try {
            statement = connection.prepareStatement(query);
            count = statement.executeUpdate();
        } finally {
            closeQuietly(statement);
        }
        assertEquals(count, 1);
    }

    @AfterClass
    public void tearDown() throws SQLException {
        Objects.requireNonNull(connection, "connection is null");
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String query = format("delete from %s", tableName);
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
    public void testGetTables() throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet rs = databaseMetaData.getTables(namespace, namespace, tableName, null);

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), tableName);
        assertFalse(rs.next());
        TestUtil.closeQuietly(rs);
    }

    @Test
    public void testGetSchemas() throws SQLException {
        ResultSet schemas = connection.getMetaData().getSchemas();

        assertTrue(schemas.next());
        String schemaName = schemas.getString(1);
        assertEquals(schemas.getString("TABLE_SCHEM"), schemaName);
        assertEquals(schemas.getString("TABLE_CATALOG"), schemaName);
        assertEquals(schemas.getString(2), schemaName);
        assertFalse(schemas.next());
        TestUtil.closeQuietly(schemas);
    }

    @Test
    public void testGetCatalogs() throws SQLException {
        ResultSet catalogs = connection.getMetaData().getCatalogs();

        assertTrue(catalogs.next());
        String catalogName = catalogs.getString(1);
        assertEquals(catalogs.getString("TABLE_CAT"), catalogName);
        assertFalse(catalogs.next());
        TestUtil.closeQuietly(catalogs);
    }
}
