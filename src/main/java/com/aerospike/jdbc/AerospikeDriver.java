package com.aerospike.jdbc;

import com.aerospike.client.Log;
import com.aerospike.jdbc.util.AerospikeClientLogger;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Logger;

import static com.aerospike.jdbc.util.Constants.DRIVER_MAJOR_VERSION;
import static com.aerospike.jdbc.util.Constants.DRIVER_MINOR_VERSION;
import static java.util.stream.Collectors.toList;

public class AerospikeDriver implements Driver {

    private static final Logger logger = Logger.getLogger("com.aerospike.jdbc");
    private static final String AEROSPIKE_URL_PREFIX = "jdbc:aerospike:";

    static {
        try {
            java.sql.DriverManager.registerDriver(new AerospikeDriver(), new AerospikeDriverAction());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError("Can not register AerospikeDriver");
        }
        logger.info("Set callback for Java client logs");
        Log.Callback asLoggerCallback = new AerospikeClientLogger();
        Log.setCallback(asLoggerCallback);
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null) {
            throw new SQLException("url is null");
        }
        if (!url.startsWith(AEROSPIKE_URL_PREFIX)) {
            return null;
        }

        return new AerospikeConnection(url, info);
    }

    public boolean acceptsURL(String url) {
        return Objects.nonNull(url) && url.startsWith(AEROSPIKE_URL_PREFIX);
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        int questionPos = url.indexOf('?');
        Collection<DriverPropertyInfo> allInfo = new ArrayList<>();
        if (questionPos > 0 && questionPos < url.length() - 1) {
            Arrays.stream(url.substring(questionPos + 1).split("&")).forEach(p -> {
                String[] kv = p.split("=");
                allInfo.add(new DriverPropertyInfo(kv[0], kv.length > 1 ? kv[1] : null));
            });
        }
        allInfo.addAll(info.entrySet().stream()
                .map(e -> new DriverPropertyInfo((String) e.getKey(), (String) e.getValue()))
                .collect(toList()));
        return allInfo.toArray(new DriverPropertyInfo[0]);
    }

    public int getMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    public int getMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    public boolean jdbcCompliant() {
        return false;
    }

    public Logger getParentLogger() {
        return logger;
    }
}
