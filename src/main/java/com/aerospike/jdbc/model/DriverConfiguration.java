package com.aerospike.jdbc.model;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.jdbc.async.EventLoopProvider;
import com.aerospike.jdbc.tls.AerospikeTLSPolicyBuilder;
import com.aerospike.jdbc.tls.AerospikeTLSPolicyConfig;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class DriverConfiguration {

    private static final Logger logger = Logger.getLogger(DriverConfiguration.class.getName());

    private static final String DEFAULT_AEROSPIKE_PORT = "3000";

    private static final Pattern AEROSPIKE_JDBC_URL = Pattern.compile("^jdbc:aerospike:(?://)?([^/?]+)");
    private static final Pattern AEROSPIKE_JDBC_CATALOG = Pattern.compile("/([^?]+)");

    private final Map<Object, Object> clientInfo = new ConcurrentHashMap<>();
    private volatile IAerospikeClient client;
    private volatile String catalog;
    private volatile ClientPolicy clientPolicy;
    private volatile DriverPolicy driverPolicy;

    public DriverConfiguration(Properties props) {
        logger.info(() -> format("Init DriverConfiguration with properties: %s", props));
        clientInfo.putAll(props);
    }

    @SuppressWarnings("java:S2696")
    public IAerospikeClient parse(String url) {
        logger.info(() -> format("Parse URL: %s", url));
        catalog = parseCatalog(url);
        updateClientInfo(url);

        Value.UseBoolBin = Optional.ofNullable(clientInfo.get("useBoolBin"))
                .map(Object::toString).map(Boolean::parseBoolean).orElse(true);
        logger.info(() -> format("Value.UseBoolBin = %b", Value.UseBoolBin));

        clientPolicy = buildClientPolicy();
        Host[] hosts = parseHosts(url, Optional.ofNullable(clientInfo.get("tlsName"))
                .map(Object::toString).orElse(null));
        client = new AerospikeClient(clientPolicy, hosts);
        resetPolicies();
        return client;
    }

    private ClientPolicy buildClientPolicy() {
        ClientPolicy policy = copy(new ClientPolicy());
        policy.eventLoops = EventLoopProvider.getEventLoops();
        policy.tlsPolicy = buildTlsPolicy();
        return policy;
    }

    private void resetPolicies() {
        logger.fine(() -> "resetPolicies call");
        if (client != null) {
            copy(client.getReadPolicyDefault());
            copy(client.getWritePolicyDefault());
            copy(client.getScanPolicyDefault());
            copy(client.getQueryPolicyDefault());
            copy(client.getBatchPolicyDefault());
            copy(client.getInfoPolicyDefault());
        }
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
        Matcher m = AEROSPIKE_JDBC_URL.matcher(url);
        if (!m.find()) {
            throw new IllegalArgumentException("Cannot parse URL " + url);
        }
        return Arrays.stream(m.group(1).split(","))
                .map(p -> p.split(":"))
                .map(a -> a.length > 1 ? a : new String[]{a[0], DEFAULT_AEROSPIKE_PORT})
                .map(hostPort -> new Host(hostPort[0], tlsName, Integer.parseInt(hostPort[1])))
                .toArray(Host[]::new);
    }

    private String parseCatalog(String url) {
        Matcher m = AEROSPIKE_JDBC_CATALOG.matcher(url);
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

    public String getCatalog() {
        return catalog;
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
        requireNonNull(clientPolicy, "clientPolicy is null");
        return clientPolicy;
    }

    public DriverPolicy getDriverPolicy() {
        requireNonNull(driverPolicy, "driverPolicy is null");
        return driverPolicy;
    }
}
