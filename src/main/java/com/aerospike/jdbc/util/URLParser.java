package com.aerospike.jdbc.util;

import com.aerospike.client.Host;
import com.aerospike.client.Value;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.async.EventLoopProvider;
import com.aerospike.jdbc.model.DriverPolicy;
import com.aerospike.jdbc.tls.AerospikeTLSPolicyBuilder;
import com.aerospike.jdbc.tls.AerospikeTLSPolicyConfig;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class URLParser {

    private static final Logger logger = Logger.getLogger(URLParser.class.getName());

    private static final String DEFAULT_AEROSPIKE_PORT = "3000";

    private static final Pattern AS_JDBC_URL = Pattern.compile("^jdbc:aerospike:(?://)?([^/?]+)");
    private static final Pattern AS_JDBC_SCHEMA = Pattern.compile("/([^?]+)");

    private static volatile Host[] hosts;
    private static volatile String schema;
    private static volatile Properties clientInfo;
    private static volatile ClientPolicy clientPolicy;
    private static volatile WritePolicy writePolicy;
    private static volatile ScanPolicy scanPolicy;
    private static volatile QueryPolicy queryPolicy;
    private static volatile DriverPolicy driverPolicy;

    private URLParser() {
    }

    public static Host[] getHosts() {
        return hosts;
    }

    public static String getSchema() {
        return schema;
    }

    public static Properties getClientInfo() {
        return clientInfo;
    }

    public static ClientPolicy getClientPolicy() {
        return clientPolicy;
    }

    public static WritePolicy getWritePolicy() {
        return writePolicy;
    }

    public static ScanPolicy getScanPolicy() {
        return scanPolicy;
    }

    public static QueryPolicy getQueryPolicy() {
        return queryPolicy;
    }

    public static DriverPolicy getDriverPolicy() {
        return driverPolicy;
    }

    public static void parseUrl(String url, Properties props) {
        logger.info(() -> "URL properties: " + props);
        schema = parseSchema(url);
        clientInfo = parseClientInfo(url, props);
        hosts = parseHosts(url, clientInfo.getProperty("tlsName"));
        clientPolicy = copy(clientInfo, new ClientPolicy());
        clientPolicy.eventLoops = EventLoopProvider.getEventLoops();
        clientPolicy.tlsPolicy = parseTlsPolicy(clientInfo);

        writePolicy = copy(clientInfo, new WritePolicy());
        scanPolicy = copy(clientInfo, new ScanPolicy());
        queryPolicy = copy(clientInfo, new QueryPolicy());
        Value.UseBoolBin = Optional.ofNullable(clientInfo.getProperty("useBoolBin"))
                .map(Boolean::parseBoolean).orElse(true);
        driverPolicy = new DriverPolicy(clientInfo);
        logger.info(() -> "Value.UseBoolBin = " + Value.UseBoolBin);
    }

    public static <T> T copy(Properties props, T object) {
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) object.getClass();
        props.forEach((key, value) -> {
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

    private static TlsPolicy parseTlsPolicy(Properties props) {
        AerospikeTLSPolicyConfig config = AerospikeTLSPolicyConfig.fromProperties(props);
        return new AerospikeTLSPolicyBuilder(config).build();
    }

    private static Host[] parseHosts(String url, final String tlsName) {
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

    private static String parseSchema(String url) {
        Matcher m = AS_JDBC_SCHEMA.matcher(url);
        return m.find() ? m.group(1) : null;
    }

    private static Properties parseClientInfo(String url, Properties props) {
        Properties all = new Properties();
        all.putAll(props);
        int questionPos = url.indexOf('?');
        if (questionPos > 0 && questionPos < url.length() - 1) {
            Arrays.stream(url.substring(questionPos + 1).split("&")).forEach(p -> {
                String[] kv = p.split("=");
                if (kv.length > 1) {
                    all.setProperty(kv[0], kv[1]);
                }
            });
        }
        return all;
    }
}
