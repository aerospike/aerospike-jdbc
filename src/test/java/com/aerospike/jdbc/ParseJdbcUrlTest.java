package com.aerospike.jdbc;

import com.aerospike.client.Value;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.jdbc.model.DriverConfiguration;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ParseJdbcUrlTest {

    @Test
    public void testParseUrlParameters() throws Exception {
        Class.forName("com.aerospike.jdbc.AerospikeDriver").newInstance();
        String url = String.format(
                "jdbc:aerospike:%s:%d/%s?%s=%d&%s=%d&%s=%b&%s=%s&%s=%d&%s=%b",
                "localhost", 3000, "test",
                "timeout", 512,
                "totalTimeout", 2000,
                "useServicesAlternate", true,
                "authMode", "external_insecure",
                "recordSetTimeoutMs", 5000,
                "useBoolBin", false
        );
        AerospikeConnection connection = (AerospikeConnection) DriverManager.getConnection(url);
        Properties properties = connection.getClientInfo();

        assertEquals(properties.getProperty("timeout"), "512");
        assertEquals(properties.getProperty("totalTimeout"), "2000");
        assertEquals(properties.getProperty("useServicesAlternate"), "true");
        assertEquals(properties.getProperty("authMode"), "external_insecure");
        assertEquals(properties.getProperty("recordSetTimeoutMs"), "5000");
        assertEquals(properties.getProperty("useBoolBin"), "false");

        DriverConfiguration config = connection.getConfiguration();
        assertEquals(config.getClientPolicy().timeout, 512);
        assertEquals(config.getQueryPolicy().totalTimeout, 2000);
        assertEquals(config.getWritePolicy().totalTimeout, 2000);
        assertEquals(config.getScanPolicy().totalTimeout, 2000);
        assertTrue(config.getClientPolicy().useServicesAlternate);
        assertEquals(config.getClientPolicy().authMode, AuthMode.EXTERNAL_INSECURE);
        assertEquals(config.getDriverPolicy().getRecordSetTimeoutMs(), 5000);
        assertFalse(Value.UseBoolBin);
        assertFalse(config.getQueryPolicy().sendKey);

        Properties update = new Properties();
        update.setProperty("totalTimeout", "3000");
        update.setProperty("sendKey", "true");
        connection.setClientInfo(update);
        assertEquals(config.getQueryPolicy().totalTimeout, 3000);
        assertEquals(config.getWritePolicy().totalTimeout, 3000);
        assertEquals(config.getScanPolicy().totalTimeout, 3000);
        assertTrue(config.getQueryPolicy().sendKey);
        assertTrue(config.getWritePolicy().sendKey);
        assertTrue(config.getScanPolicy().sendKey);

        connection.setClientInfo("recordSetTimeoutMs", "7000");
        assertEquals(config.getDriverPolicy().getRecordSetTimeoutMs(), 7000);
    }
}
