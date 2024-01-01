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
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            if (i != 1) {
                System.out.print(", ");
            }
            System.out.print(resultSetMetaData.getColumnName(i));
        }
        System.out.println();
        while (rs.next()) {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
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
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        boolean hasNext = resultSet.next();
        resultSet.close();
        statement.close();
        return hasNext;
    }

    @SuppressWarnings("all")
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }
}
