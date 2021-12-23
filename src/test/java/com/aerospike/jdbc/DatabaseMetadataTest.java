package com.aerospike.jdbc;

import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.testng.Assert.*;

public class DatabaseMetadataTest extends JdbcBaseTest {

    @Test
    @Ignore
    public void testGetTables() throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();

        ResultSet rs = databaseMetaData.getTables(namespace, namespace, tableName, null);

        assertTrue(rs.next());
        assertEquals(tableName, rs.getString("TABLE_NAME"));
        assertFalse(rs.next());
    }
}
