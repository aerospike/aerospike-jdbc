package com.aerospike.jdbc;

import org.testng.annotations.Test;

import java.sql.DriverManager;

public class ParseJdbcUrlTest {

    @Test
    public void testParseUrlParameters() throws Exception {
        Class.forName("com.aerospike.jdbc.AerospikeDriver").newInstance();
        String url = String.format(
                "jdbc:aerospike:%s:%d/%s?timeout=%d&useServicesAlternate=%b&maxRecords=%d&authMode=%s",
                "localhost", 3000, "test", 512, true, 64L, "external_insecure"
        );
        DriverManager.getConnection(url);
    }
}
