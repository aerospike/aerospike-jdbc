package com.aerospike.jdbc;

import com.aerospike.jdbc.util.TestRecord;
import com.aerospike.jdbc.util.TestUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import static com.aerospike.jdbc.util.TestConfig.NAMESPACE;
import static com.aerospike.jdbc.util.TestConfig.TABLE_NAME;
import static com.aerospike.jdbc.util.TestUtil.closeQuietly;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DatabaseMetadataTest extends JdbcBaseTest {

    private final TestRecord testRecord;

    DatabaseMetadataTest() {
        testRecord = new TestRecord("key1", true, 11100, 1, "bar");
    }

    @BeforeClass
    public void setUp() throws SQLException {
        Objects.requireNonNull(connection, "connection is null");
        PreparedStatement statement = null;
        int count;
        String query = testRecord.toInsertQuery();
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
        String query = format("delete from %s", TABLE_NAME);
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
    public void testGetTables() throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet tables = databaseMetaData.getTables(NAMESPACE, "", TABLE_NAME, null);

        if (tables.next()) {
            assertEquals(tables.getString("TABLE_NAME"), TABLE_NAME);
            assertFalse(tables.next());
        }
        TestUtil.closeQuietly(tables);
    }

    @Test
    public void testGetSchemas() throws SQLException {
        ResultSet schemas = connection.getMetaData().getSchemas();

        assertTrue(schemas.next());
        String schemaName = schemas.getString(1);
        String catalogName = schemas.getString(2);
        assertEquals(schemas.getString("TABLE_SCHEM"), schemaName);
        assertEquals(schemas.getString("TABLE_CATALOG"), catalogName);
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

    @Test
    public void testJDBCVersion() throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();

        assertEquals(metadata.getJDBCMajorVersion(), 4);
        assertEquals(metadata.getJDBCMinorVersion(), 2);
    }
}
