package com.aerospike.jdbc.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.query.IndexType;
import com.aerospike.jdbc.model.AerospikeSecondaryIndex;
import com.google.common.base.Splitter;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class AerospikeUtils {

    private static final Logger logger = Logger.getLogger(AerospikeUtils.class.getName());

    private static volatile Map<String, AerospikeSecondaryIndex> indexMap;

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

    public static Map<String, AerospikeSecondaryIndex> getSecondaryIndexes(final IAerospikeClient client) {
        if (indexMap == null) {
            synchronized (AerospikeUtils.class) {
                if (indexMap == null) {
                    logger.info("Fetching secondary indexes");

                    String indexData = Info.request(null, client.getCluster().getRandomNode(), "sindex");
                    try {
                        indexMap = Splitter.on(";").trimResults().splitToList(indexData).stream()
                                .map(index -> Splitter.on(":").trimResults().splitToList(index).stream()
                                        .map(prop -> Splitter.on("=").trimResults().splitToList(prop))
                                        .collect(Collectors.toMap(t -> t.get(0), t -> t.get(1))))
                                .filter(AerospikeUtils::isSupportedIndexType)
                                .map(i -> new AerospikeSecondaryIndex(
                                        i.get("ns"),
                                        i.get("set"),
                                        i.get("bin"),
                                        i.get("indexname"),
                                        IndexType.valueOf(i.get("type").toUpperCase(Locale.ENGLISH)),
                                        getIndexBinValuesRatio(client, i.get("ns"), i.get("indexname"))))
                                .collect(Collectors.toMap(AerospikeSecondaryIndex::toKey, Function.identity()));
                    } catch (Exception e) {
                        indexMap = Collections.emptyMap();
                        logger.info("Failed to fetch secondary indexes: " + e.getMessage());
                    }
                }
            }
        }
        return indexMap;
    }

    private static boolean isSupportedIndexType(Map<String, String> indexData) {
        String indexType = indexData.get("type");
        return indexType.equalsIgnoreCase(IndexType.NUMERIC.toString())
                || indexType.equalsIgnoreCase(IndexType.STRING.toString());
    }

    private static Integer getIndexBinValuesRatio(IAerospikeClient client, String namespace, String indexName) {
        if (VersionUtils.isSIndexCardinalitySupported(client)) {
            try {
                String indexStatData = Info.request(null, client.getCluster().getRandomNode(),
                        String.format("sindex-stat:ns=%s;indexname=%s", namespace, indexName));

                return Integer.valueOf(Splitter.on(";").trimResults().splitToList(indexStatData).stream()
                        .map(stat -> Splitter.on("=").trimResults().splitToList(stat))
                        .collect(Collectors.toMap(t -> t.get(0), t -> t.get(1)))
                        .get("entries_per_bval"));
            } catch (Exception e) {
                logger.warning(String.format("Failed to fetch secondary index %s cardinality", indexName));
            }
        }
        return null;
    }
}
