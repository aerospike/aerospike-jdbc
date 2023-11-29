package com.aerospike.jdbc.model;

import com.aerospike.client.Host;
import com.aerospike.client.Value;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.async.EventLoopProvider;
import com.aerospike.jdbc.tls.AerospikeTLSPolicyBuilder;
import com.aerospike.jdbc.tls.AerospikeTLSPolicyConfig;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DriverConfiguration {

    private static final Logger logger = Logger.getLogger(DriverConfiguration.class.getName());

    private static final String DEFAULT_AEROSPIKE_PORT = "3000";

    private static final Pattern AS_JDBC_URL = Pattern.compile("^jdbc:aerospike:(?://)?([^/?]+)");
    private static final Pattern AS_JDBC_SCHEMA = Pattern.compile("/([^?]+)");

    private final ConcurrentHashMap<Object, Object> clientInfo = new ConcurrentHashMap<>();
    private Host[] hosts;
    private String schema;
    private volatile ClientPolicy clientPolicy;
    private volatile WritePolicy writePolicy;
    private volatile ScanPolicy scanPolicy;
    private volatile QueryPolicy queryPolicy;
    private volatile DriverPolicy driverPolicy;

    public DriverConfiguration(Properties props) {
        logger.info(() -> "Configuration properties: " + props);
        clientInfo.putAll(props);
    }

    @SuppressWarnings("java:S2696")
    public void parse(String url) {
        schema = parseSchema(url);
        updateClientInfo(url);
        hosts = parseHosts(url, Optional.ofNullable(clientInfo.get("tlsName"))
                .map(Object::toString).orElse(null));
        resetPolicies();
        Value.UseBoolBin = Optional.ofNullable(clientInfo.get("useBoolBin"))
                .map(Object::toString).map(Boolean::parseBoolean).orElse(true);
        logger.info(() -> "Value.UseBoolBin = " + Value.UseBoolBin);
    }

    private void resetPolicies() {
        clientPolicy = copy(new ClientPolicy());
        clientPolicy.eventLoops = EventLoopProvider.getEventLoops();
        clientPolicy.tlsPolicy = buildTlsPolicy();

        writePolicy = copy(new WritePolicy());
        scanPolicy = copy(new ScanPolicy());
        queryPolicy = copy(new QueryPolicy());
        driverPolicy = new DriverPolicy(getClientInfo());
    }

    private <T> T copy(T object) {
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) object.getClass();
        clientInfo.forEach((key, value) -> {
            try {
                Field field = clazz.getField((String) key);
                if (field.getType().equals(Integer.TYPE)) {
                    field.set(object, Integer.valueOf(value.toString()));
                } else if (field.getType().equals(Long.TYPE)) {
                    field.set(object, Long.valueOf(value.toString()));
                } else if (field.getType().equals(Boolean.TYPE)) {
                    field.set(object, Boolean.valueOf(value.toString()));
                } else if (field.getType().equals(AuthMode.class)) {
                    field.set(object, AuthMode.valueOf(value.toString().toUpperCase(Locale.ENGLISH)));
                } else {
                    field.set(object, value);
                }
            } catch (ReflectiveOperationException e1) {
                // ignore it; this property does not belong to the object
            }
        });
        return object;
    }

    private TlsPolicy buildTlsPolicy() {
        AerospikeTLSPolicyConfig config = AerospikeTLSPolicyConfig.fromProperties(getClientInfo());
        return new AerospikeTLSPolicyBuilder(config).build();
    }

    private Host[] parseHosts(String url, final String tlsName) {
        Matcher m = AS_JDBC_URL.matcher(url);
        if (!m.find()) {
            throw new IllegalArgumentException("Cannot parse URL " + url);
        }
        return Arrays.stream(m.group(1).split(","))
                .map(p -> p.split(":"))
                .map(a -> a.length > 1 ? a : new String[]{a[0], DEFAULT_AEROSPIKE_PORT})
                .map(hostPort -> new Host(hostPort[0], tlsName, Integer.parseInt(hostPort[1])))
                .toArray(Host[]::new);
    }

    private String parseSchema(String url) {
        Matcher m = AS_JDBC_SCHEMA.matcher(url);
        return m.find() ? m.group(1) : null;
    }

    private void updateClientInfo(String url) {
        int questionPos = url.indexOf('?');
        if (questionPos > 0 && questionPos < url.length() - 1) {
            Arrays.stream(url.substring(questionPos + 1).split("&")).forEach(p -> {
                String[] kv = p.split("=");
                if (kv.length > 1) {
                    clientInfo.put(kv[0], kv[1]);
                }
            });
        }
    }

    public Host[] getHosts() {
        return hosts;
    }

    public String getSchema() {
        return schema;
    }

    public Properties getClientInfo() {
        Properties properties = new Properties();
        properties.putAll(clientInfo);
        return properties;
    }

    public void put(String name, String value) {
        clientInfo.put(name, value);
        resetPolicies();
    }

    public void putAll(Properties properties) {
        clientInfo.putAll(properties);
        resetPolicies();
    }

    public ClientPolicy getClientPolicy() {
        return clientPolicy;
    }

    public WritePolicy getWritePolicy() {
        return writePolicy;
    }

    public ScanPolicy getScanPolicy() {
        return scanPolicy;
    }

    public QueryPolicy getQueryPolicy() {
        return queryPolicy;
    }

    public DriverPolicy getDriverPolicy() {
        return driverPolicy;
    }
}
