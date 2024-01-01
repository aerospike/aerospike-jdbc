package com.aerospike.jdbc.util;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static com.aerospike.jdbc.util.TestConfig.TABLE_NAME;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestRecord {

    private final String primaryKey;
    private final boolean bool1;
    private final int int1;
    private final int int2;
    private final String str1;
    private final byte[] val1;
    private final Object val2;

    public TestRecord(
            String primaryKey,
            boolean bool1,
            int int1,
            int int2,
            String str1
    ) {
        this.primaryKey = primaryKey;
        this.bool1 = bool1;
        this.int1 = int1;
        this.int2 = int2;
        this.str1 = str1;
        this.val1 = null;
        this.val2 = null;
    }

    public TestRecord(
            String primaryKey,
            boolean bool1,
            int int1,
            int int2,
            String str1,
            byte[] val1,
            Object val2
    ) {
        this.primaryKey = primaryKey;
        this.bool1 = bool1;
        this.int1 = int1;
        this.int2 = int2;
        this.str1 = str1;
        this.val1 = val1;
        this.val2 = val2;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public boolean getBool1() {
        return bool1;
    }

    public byte[] getVal1() {
        return val1;
    }

    public int getInt1() {
        return int1;
    }

    public int getInt2() {
        return int2;
    }

    public String getStr1() {
        return str1;
    }

    public String toInsertQuery() {
        return format(
                "INSERT INTO %s (%s, bool1, int1, int2, str1) VALUES (%s, %b, %d, %d, %s)",
                TABLE_NAME,
                PRIMARY_KEY_COLUMN_NAME,
                primaryKey,
                bool1,
                int1,
                int2,
                str1
        );
    }

    public String toPreparedInsertQuery() {
        return format(
                "INSERT INTO %s (%s, bool1, int1, int2, str1, val1, val2) VALUES (%s, %b, %d, %d, %s, ?, ?)",
                TABLE_NAME,
                PRIMARY_KEY_COLUMN_NAME,
                primaryKey,
                bool1,
                int1,
                int2,
                str1
        );
    }

    public void assertResultSet(ResultSet resultSet) throws SQLException {
        assertResultSet(resultSet, true);
    }

    public void assertResultSet(ResultSet resultSet, boolean sendKey) throws SQLException {
        assertEquals(resultSet.getString(PRIMARY_KEY_COLUMN_NAME), sendKey ? primaryKey : null);
        assertEquals(resultSet.getBoolean("bool1"), bool1);
        assertEquals(resultSet.getInt("int1"), int1);
        assertEquals(resultSet.getInt("int2"), int2);
        assertEquals(resultSet.getString("str1"), str1);

        assertEquals(resultSet.getString(1), sendKey ? primaryKey : null);
        assertEquals(resultSet.getBoolean(2), bool1);
        assertEquals(resultSet.getInt(3), int1);
        assertEquals(resultSet.getInt(4), int2);
        assertEquals(resultSet.getString(5), str1);
    }

    public void assertPreparedResultSet(ResultSet resultSet) throws SQLException {
        assertResultSet(resultSet);
        assertEquals(resultSet.getBytes("val1"), val1);
        assertEquals(resultSet.getBytes(6), val1);
    }
}
