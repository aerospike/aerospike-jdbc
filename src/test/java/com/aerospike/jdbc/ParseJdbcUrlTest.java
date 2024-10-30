package com.aerospike.jdbc;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.jdbc.model.DriverConfiguration;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
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
        IAerospikeClient client = connection.getClient();

        assertEquals(properties.getProperty("timeout"), "512");
        assertEquals(properties.getProperty("totalTimeout"), "2000");
        assertEquals(properties.getProperty("useServicesAlternate"), "true");
        assertEquals(properties.getProperty("authMode"), "external_insecure");
        assertEquals(properties.getProperty("recordSetTimeoutMs"), "5000");
        assertEquals(properties.getProperty("useBoolBin"), "false");

        DriverConfiguration config = connection.getConfiguration();
        assertEquals(config.getClientPolicy().timeout, 512);
        assertEquals(client.getInfoPolicyDefault().timeout, 512);
        assertEquals(client.getScanPolicyDefault().recordsPerSecond, 0);
        assertTotalTimeoutAll(client, 2000);
        assertSendKeyAll(client, false);
        assertTrue(config.getClientPolicy().useServicesAlternate);
        assertEquals(config.getClientPolicy().authMode, AuthMode.EXTERNAL_INSECURE);
        assertEquals(config.getDriverPolicy().getRecordSetTimeoutMs(), 5000);
        assertFalse(Value.UseBoolBin);
        Value.UseBoolBin = true;

        Properties update = new Properties();
        update.setProperty("totalTimeout", "3000");
        update.setProperty("sendKey", "true");
        update.setProperty("recordSetQueueCapacity", "1024");
        update.setProperty("metadataCacheTtlSeconds", "7200");
        update.setProperty("recordsPerSecond", "128");
        update.setProperty("schemaBuilderMaxRecords", "500");
        connection.setClientInfo(update);
        assertEquals(client.getScanPolicyDefault().recordsPerSecond, 128);
        assertTotalTimeoutAll(client, 3000);
        assertSendKeyAll(client, true);
        assertEquals(config.getDriverPolicy().getRecordSetQueueCapacity(), 1024);
        assertEquals(config.getDriverPolicy().getMetadataCacheTtlSeconds(), 7200);
        assertEquals(config.getDriverPolicy().getSchemaBuilderMaxRecords(), 500);

        connection.setClientInfo("recordSetTimeoutMs", "7000");
        assertEquals(config.getDriverPolicy().getRecordSetTimeoutMs(), 7000);
    }

    @Test
    public void testInapplicableUrl() throws Exception {
        AerospikeDriver driver = (AerospikeDriver) Class.forName("com.aerospike.jdbc.AerospikeDriver")
                .newInstance();
        String url = "jdbc:postgresql://localhost:5432/sample";
        assertNull(driver.connect(url, new Properties()));
    }

    private void assertTotalTimeoutAll(IAerospikeClient client, int timeout) {
        assertEquals(client.getReadPolicyDefault().totalTimeout, timeout);
        assertEquals(client.getWritePolicyDefault().totalTimeout, timeout);
        assertEquals(client.getQueryPolicyDefault().totalTimeout, timeout);
        assertEquals(client.getScanPolicyDefault().totalTimeout, timeout);
        assertEquals(client.getBatchPolicyDefault().totalTimeout, timeout);
    }

    private void assertSendKeyAll(IAerospikeClient client, boolean sendKey) {
        assertEquals(client.getReadPolicyDefault().sendKey, sendKey);
        assertEquals(client.getWritePolicyDefault().sendKey, sendKey);
        assertEquals(client.getQueryPolicyDefault().sendKey, sendKey);
        assertEquals(client.getScanPolicyDefault().sendKey, sendKey);
        assertEquals(client.getBatchPolicyDefault().sendKey, sendKey);
    }
}
