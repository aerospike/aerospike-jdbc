package com.aerospike.jdbc;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

public abstract class JdbcBaseTest {

    protected static final String namespace = "test";
    protected static final String tableName = "jdbc";

    private static final Logger logger = Logger.getLogger(JdbcBaseTest.class.getName());
    private static final String hostname = "localhost";
    private static final int port = 3000;

    protected static Connection connection;

    @BeforeSuite
    public static void connectionInit() throws Exception {
        logger.info("connectionInit");
        Class.forName("com.aerospike.jdbc.AerospikeDriver").newInstance();
        String url = String.format("jdbc:aerospike:%s:%d/%s?sendKey=true", hostname, port, namespace);
        connection = DriverManager.getConnection(url);
    }

    @AfterSuite
    public static void connectionClose() throws SQLException {
        logger.info("connectionClose");
        connection.close();
    }
}
