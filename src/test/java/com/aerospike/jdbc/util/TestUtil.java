package com.aerospike.jdbc.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public final class TestUtil {

    private TestUtil() {
    }

    public static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            if (i != 1) {
                System.out.print(", ");
            }
            System.out.print(rsmd.getColumnName(i));
        }
        System.out.println();
        while (rs.next()) {
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                if (i != 1) {
                    System.out.print(", ");
                }
                System.out.print(rs.getString(i));
            }
            System.out.println();
        }
    }

    public static void closeQuietly(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception ignore) {
            }
        }
    }

    public static boolean executeQuery(Connection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        boolean hasNext = rs.next();
        rs.close();
        stmt.close();
        return hasNext;
    }
}
