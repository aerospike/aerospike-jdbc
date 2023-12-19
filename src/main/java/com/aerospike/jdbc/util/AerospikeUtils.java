package com.aerospike.jdbc.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.jdbc.model.AerospikeClusterInfo;
import com.aerospike.jdbc.model.AerospikeSecondaryIndex;
import com.google.common.base.Splitter;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aerospike.jdbc.util.Constants.DEFAULT_SCHEMA_NAME;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.synchronizedSet;

public final class AerospikeUtils {

    private static final Logger logger = Logger.getLogger(AerospikeUtils.class.getName());

    private static final String NEW_LINE = System.lineSeparator();
    private static final String NOT_AVAILABLE = "NA";

    private AerospikeUtils() {
    }

    public static Map<String, String> getTableInfo(InfoPolicy infoPolicy, String ns, String set, Node node) {
        String sets = Info.request(infoPolicy, node, "sets");
        Optional<String> tableInfo = Splitter.on(";").trimResults().splitToList(sets).stream()
                .filter(s -> s.startsWith("ns=" + ns + ":set=" + set))
                .findFirst();

        return tableInfo.map(value -> Pattern.compile("\\s*:\\s*")
                .splitAsStream(value)
                .map(s -> s.split("=", 2))
                .collect(Collectors.toMap(e -> e[0], e -> e[1]))).orElse(null);
    }

    public static Map<String, String> getSchemaInfo(IAerospikeClient client, String ns) {
        String schemaInfo = Info.request(client.getInfoPolicyDefault(),
                client.getCluster().getRandomNode(), "namespace/" + ns);

        return Splitter.on(";").trimResults().splitToList(schemaInfo).stream()
                .map(s -> s.split("=", 2))
                .collect(Collectors.toMap(e -> e[0], e -> e[1]));
    }

    public static int getTableRecordsNumber(IAerospikeClient client, String ns, String set) {
        int allRecords = Arrays.stream(client.getNodes())
                .map(n -> getTableInfo(client.getInfoPolicyDefault(), ns, set, n))
                .map(m -> Integer.parseInt(m.get("objects")))
                .reduce(0, Integer::sum);

        int replicationFactor = Integer.parseInt(getSchemaInfo(client, ns).get("effective_replication_factor"));

        return (int) Math.floor((double) allRecords / replicationFactor);
    }

    public static AerospikeClusterInfo getClusterInfo(IAerospikeClient client) {
        final Collection<String> builds = synchronizedSet(new HashSet<>());
        final Collection<String> editions = synchronizedSet(new HashSet<>());
        final Collection<String> namespaces = synchronizedSet(new HashSet<>());
        final Map<String, Collection<String>> tables = new ConcurrentHashMap<>();
        try {
            Arrays.stream(client.getNodes()).parallel()
                    .map(node -> Info.request(client.getInfoPolicyDefault(), node,
                            "namespaces", "sets", "sindex", "build", "edition"))
                    .forEach(info -> {
                        builds.add(info.get("build"));
                        editions.add(info.get("edition"));
                        Optional.ofNullable(info.get("namespaces"))
                                .map(ns -> Arrays.stream(ns.split(";")).filter(n -> !n.isEmpty())
                                        .collect(Collectors.toList()))
                                .ifPresent(namespaces::addAll);

                        streamSubProperties(info, "sets").forEach(p ->
                                tables.computeIfAbsent(p.getProperty("ns"), s -> new HashSet<>())
                                        .addAll(Arrays.asList(p.getProperty("set"), DEFAULT_SCHEMA_NAME))
                        );
                    });
        } catch (Exception e) {
            logger.log(Level.WARNING, "Exception in getClusterInfo", e);
        }
        return new AerospikeClusterInfo(
                builds.isEmpty() ? NOT_AVAILABLE : join(", ", builds),
                editions.isEmpty() ? "Aerospike" : join(", ", editions),
                Collections.unmodifiableCollection(namespaces),
                Collections.unmodifiableMap(tables)
        );
    }

    public static Map<String, Collection<AerospikeSecondaryIndex>> getCatalogIndexes(
            IAerospikeClient client,
            AerospikeVersion aerospikeVersion
    ) {
        final InfoPolicy infoPolicy = client.getInfoPolicyDefault();
        final Map<String, Collection<AerospikeSecondaryIndex>> catalogIndexes = new HashMap<>();
        try {
            String indexInfo = Info.request(infoPolicy, client.getCluster().getRandomNode(), "sindex");
            streamSubProperties(indexInfo).filter(AerospikeUtils::isSupportedIndexType)
                    .forEach(index -> {
                        String namespace = index.getProperty("ns");
                        String indexName = index.getProperty("indexname");
                        Integer binRatio = aerospikeVersion.isSIndexCardinalitySupported()
                                ? getIndexBinValuesRatio(client, namespace, indexName)
                                : null;
                        catalogIndexes.computeIfAbsent(namespace, s -> new HashSet<>())
                                .add(new AerospikeSecondaryIndex(
                                        namespace,
                                        index.getProperty("set"),
                                        index.getProperty("bin"),
                                        indexName,
                                        IndexType.valueOf(index.getProperty("type").toUpperCase(Locale.ENGLISH)),
                                        binRatio)
                                );
                    });
        } catch (Exception e) {
            logger.log(Level.WARNING, "Exception in getCatalogIndexes", e);
        }
        return Collections.unmodifiableMap(catalogIndexes);
    }

    @SuppressWarnings("SameParameterValue")
    private static Stream<Properties> streamSubProperties(Map<String, String> info, String key) {
        return streamSubProperties(Optional.ofNullable(info.get(key)).orElse(""));
    }

    private static Stream<Properties> streamSubProperties(String info) {
        return Arrays.stream(info.split(";"))
                .filter(str -> !str.isEmpty())
                .map(str -> str.replace(":", NEW_LINE))
                .map(str -> {
                    Properties properties = new Properties();
                    try {
                        properties.load(new StringReader(str));
                    } catch (IOException e) {
                        logger.log(Level.WARNING, format("Failed to load properties: %s", str), e);
                    }
                    return properties;
                });
    }

    public static boolean isSupportedIndexType(Properties properties) {
        String indexType = properties.getProperty("type");
        return indexType.equalsIgnoreCase(IndexType.NUMERIC.toString())
                || indexType.equalsIgnoreCase(IndexType.STRING.toString());
    }

    public static Integer getIndexBinValuesRatio(IAerospikeClient client, String namespace, String indexName) {
        try {
            String indexStatData = Info.request(client.getInfoPolicyDefault(), client.getCluster().getRandomNode(),
                    format("sindex-stat:ns=%s;indexname=%s", namespace, indexName));

            return Integer.valueOf(Splitter.on(";").trimResults().splitToList(indexStatData).stream()
                    .map(stat -> Splitter.on("=").trimResults().splitToList(stat))
                    .collect(Collectors.toMap(t -> t.get(0), t -> t.get(1)))
                    .get("entries_per_bval"));
        } catch (Exception e) {
            logger.log(Level.WARNING, format("Failed to fetch secondary index %s cardinality", indexName), e);
            return null;
        }
    }
}
