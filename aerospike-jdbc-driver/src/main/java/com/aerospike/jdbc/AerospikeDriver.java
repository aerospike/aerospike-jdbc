package com.aerospike.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;

import static java.util.stream.Collectors.toList;

public class AerospikeDriver implements Driver {

    private static final Logger logger = Logger.getLogger("com.aerospike.jdbc");

    static {
        try {
            java.sql.DriverManager.registerDriver(new AerospikeDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError("Can not register AerospikeDriver");
        }
    }

    public Connection connect(String url, Properties info) {
        return new AerospikeConnection(url, info);
    }

    public boolean acceptsURL(String url) {
        return Objects.nonNull(url) && url.startsWith("jdbc:aerospike:");
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
        allInfo.addAll(info.entrySet().stream().map(e -> new DriverPropertyInfo((String) e.getKey(),
                (String) e.getValue())).collect(toList()));
        return allInfo.toArray(new DriverPropertyInfo[0]);
    }

    public int getMajorVersion() {
        return 1;
    }

    public int getMinorVersion() {
        return 0;
    }

    public boolean jdbcCompliant() {
        return false;
    }

    public Logger getParentLogger() {
        return logger;
    }
}
