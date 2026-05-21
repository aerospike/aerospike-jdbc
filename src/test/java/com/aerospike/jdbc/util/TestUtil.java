package com.aerospike.jdbc.util;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class TestUtil {

    private static final Logger logger = Logger.getLogger(TestUtil.class.getName());

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

    /**
     * Reads {@code namespace/&lt;ns&gt;} info and returns whether {@code strong-consistency} is enabled
     * (same field as server {@code cfg_info.c}).
     */
    public static boolean isStrongConsistencyNamespace(String hostname, int port, String namespace) {
        AerospikeClient probe = null;
        try {
            probe = new AerospikeClient(hostname, port);
            String raw = Info.request(
                    probe.getInfoPolicyDefault(),
                    probe.getCluster().getRandomNode(),
                    "namespace/" + namespace);
            return namespaceInfoHasStrongConsistency(raw);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Could not read namespace info for SC detection: " + namespace, e);
            return false;
        } finally {
            if (probe != null) {
                probe.close();
            }
        }
    }

    /**
     * Returns {@code &durableDelete=true} for JDBC URLs when the namespace runs in strong consistency mode.
     */
    public static String durableDeleteUrlSuffixIfStrongConsistency(String hostname, int port, String namespace) {
        return isStrongConsistencyNamespace(hostname, port, namespace) ? "&durableDelete=true" : "";
    }

    static boolean namespaceInfoHasStrongConsistency(String raw) {
        if (raw == null || raw.isEmpty()) {
            return false;
        }
        for (String part : raw.split(";")) {
            String[] kv = part.split("=", 2);
            if (kv.length != 2) {
                continue;
            }
            String key = kv[0].trim().toLowerCase(Locale.ROOT);
            String val = kv[1].trim().toLowerCase(Locale.ROOT);
            if ("strong-consistency".equals(key) && ("true".equals(val) || "yes".equals(val))) {
                return true;
            }
        }
        return false;
    }
}
