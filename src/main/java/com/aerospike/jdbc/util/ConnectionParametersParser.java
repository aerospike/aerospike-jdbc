package com.aerospike.jdbc.util;

import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.jdbc.scan.EventLoopProvider;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.join;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class ConnectionParametersParser {

    private static final String defaultAerospikePort = "3000";

    private static final Pattern AS_JDBC_URL = Pattern.compile("^jdbc:aerospike:(?://)?([^/?]+)");
    private static final Pattern AS_JDBC_SCHEMA = Pattern.compile("/([^?]+)");

    public ClientPolicy policy(String url, Properties info) {
        ClientPolicy policy = new ClientPolicy();
        policy.eventLoops = EventLoopProvider.getEventLoops();
        return copy(clientInfo(url, info), policy);
    }

    public static <T> T copy(Properties props, T object) {
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) object.getClass();
        props.forEach((key, value) -> {
            try {
                clazz.getField((String) key).set(object, value);
            } catch (ReflectiveOperationException e1) {
                // ignore it; this property does not belong to policy
            }
        });
        return object;
    }

    public Host[] hosts(String url) {
        Matcher m = AS_JDBC_URL.matcher(url);
        if (!m.find()) {
            throw new IllegalArgumentException("Cannot parse URL " + url);
        }
        return Arrays.stream(m.group(1).split(","))
                .map(p -> p.split(":")).map(a -> a.length > 1 ? a : new String[]{a[0], defaultAerospikePort})
                .map(hostPort -> new Host(hostPort[0], Integer.parseInt(hostPort[1])))
                .toArray(Host[]::new);
    }

    public String schema(String url) {
        Matcher m = AS_JDBC_SCHEMA.matcher(url);
        return m.find() ? m.group(1) : null;
    }

    public Properties clientInfo(String url, Properties info) {
        Properties all = new Properties();
        all.putAll(info);
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

    public Properties subProperties(Properties properties, String prefix) {
        String filter = prefix.endsWith(".") ? prefix : prefix + ".";
        int prefixLength = filter.length();
        Properties result = new Properties();
        result.putAll(properties.entrySet().stream()
                .filter(e -> ((String) e.getKey()).startsWith(filter))
                .collect(toMap(e -> ((String) e.getKey()).substring(prefixLength), Map.Entry::getValue)));
        return result;
    }

    public Collection<String> indexesParser(String infos) {
        return indexesParser(infos, "type", "ns", "set", "bin", "indexname");
    }

    public Collection<String> indexesParser(String infos, String... propNames) {
        return Arrays.stream(infos.split(";"))
                .filter(info -> !info.isEmpty())
                .map(info -> new StringReader(info.replace(":", "\n")))
                .map(r -> {
                    Properties props = new Properties();
                    try {
                        props.load(r);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                    return props;
                }).map(p -> join(".", Arrays.stream(propNames)
                        .map(p::getProperty).toArray(String[]::new)))
                .collect(toSet());
    }
}
