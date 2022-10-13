package com.aerospike.jdbc.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.query.IndexType;
import com.google.common.base.Splitter;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class AerospikeUtils {

    private static final Logger logger = Logger.getLogger(AerospikeUtils.class.getName());

    private AerospikeUtils() {
    }

    public static Map<String, String> getTableInfo(String ns, String set, Node node) {
        String sets = Info.request(null, node, "sets");
        Optional<String> tableInfo = Splitter.on(";").trimResults().splitToList(sets).stream()
                .filter(s -> s.startsWith("ns=" + ns + ":set=" + set))
                .findFirst();
        return tableInfo.map(value -> Pattern.compile("\\s*:\\s*")
                .splitAsStream(value)
                .map(s -> s.split("=", 2))
                .collect(Collectors.toMap(e -> e[0], e -> e[1]))).orElse(null);
    }

    public static Map<String, String> getSchemaInfo(IAerospikeClient client, String ns) {
        String schemaInfo = Info.request(null, client.getCluster().getRandomNode(), "namespace/" + ns);
        return Splitter.on(";").trimResults().splitToList(schemaInfo).stream()
                .map(s -> s.split("=", 2))
                .collect(Collectors.toMap(e -> e[0], e -> e[1]));
    }

    public static int getTableRecordsNumber(IAerospikeClient client, String ns, String set) {
        int allRecords = Arrays.stream(client.getNodes()).map(n -> getTableInfo(ns, set, n))
                .map(m -> Integer.parseInt(m.get("objects")))
                .reduce(0, Integer::sum);

        int replicationFactor = Integer.parseInt(getSchemaInfo(client, ns).get("effective_replication_factor"));

        return (int) Math.floor((double) allRecords / replicationFactor);
    }

    public static boolean isSupportedIndexType(Properties properties) {
        String indexType = properties.getProperty("type");
        return indexType.equalsIgnoreCase(IndexType.NUMERIC.toString())
                || indexType.equalsIgnoreCase(IndexType.STRING.toString());
    }

    public static Integer getIndexBinValuesRatio(IAerospikeClient client, String namespace, String indexName) {
        if (VersionUtils.isSIndexCardinalitySupported(client)) {
            try {
                String indexStatData = Info.request(null, client.getCluster().getRandomNode(),
                        String.format("sindex-stat:ns=%s;indexname=%s", namespace, indexName));

                return Integer.valueOf(Splitter.on(";").trimResults().splitToList(indexStatData).stream()
                        .map(stat -> Splitter.on("=").trimResults().splitToList(stat))
                        .collect(Collectors.toMap(t -> t.get(0), t -> t.get(1)))
                        .get("entries_per_bval"));
            } catch (Exception e) {
                logger.warning(() -> String.format("Failed to fetch secondary index %s cardinality", indexName));
            }
        }
        return null;
    }
}
