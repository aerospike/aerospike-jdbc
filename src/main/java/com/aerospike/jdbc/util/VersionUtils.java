package com.aerospike.jdbc.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;

import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

public final class VersionUtils {

    private static final Logger logger = Logger.getLogger(VersionUtils.class.getName());

    private static final String S_INDEX_SUPPORT_VERSION = "6.0.0.0";
    private static final String BATCH_OPS_SUPPORT_VERSION = "6.0.0.0";
    private static final String S_INDEX_CARDINALITY_SUPPORT_VERSION = "6.1.0.0";
    private static final Pattern versionPattern = Pattern.compile("^(\\d.){1,3}\\d(?=.*|$)");

    private static volatile Boolean sIndexSupported;
    private static volatile Boolean batchOpsSupported;
    private static volatile Boolean sIndexCardinalitySupported;

    private VersionUtils() {
    }

    public static boolean isSIndexSupported(IAerospikeClient client) {
        if (sIndexSupported == null) {
            synchronized (VersionUtils.class) {
                if (sIndexSupported == null) {
                    String serverVersion = clearQualifier(getAerospikeServerVersion(client));
                    sIndexSupported = compareVersions(serverVersion, S_INDEX_SUPPORT_VERSION) >= 0;
                    logger.info(() -> format("Secondary index supported: %b, for version: %s",
                            sIndexSupported, serverVersion));
                }
            }
        }
        return sIndexSupported;
    }

    public static boolean isBatchOpsSupported(IAerospikeClient client) {
        if (batchOpsSupported == null) {
            synchronized (VersionUtils.class) {
                if (batchOpsSupported == null) {
                    String serverVersion = clearQualifier(getAerospikeServerVersion(client));
                    batchOpsSupported = compareVersions(serverVersion, BATCH_OPS_SUPPORT_VERSION) >= 0;
                    logger.info(() -> format("Batch operations supported: %b, for version: %s",
                            batchOpsSupported, serverVersion));
                }
            }
        }
        return batchOpsSupported;
    }

    public static boolean isSIndexCardinalitySupported(IAerospikeClient client) {
        if (sIndexCardinalitySupported == null) {
            synchronized (VersionUtils.class) {
                if (sIndexCardinalitySupported == null) {
                    String serverVersion = clearQualifier(getAerospikeServerVersion(client));
                    sIndexCardinalitySupported = compareVersions(serverVersion, S_INDEX_CARDINALITY_SUPPORT_VERSION) >= 0;
                    logger.info(() -> format("Secondary index cardinality supported: %b, for version: %s",
                            sIndexCardinalitySupported, serverVersion));
                }
            }
        }
        return sIndexCardinalitySupported;
    }

    public static String getAerospikeServerVersion(IAerospikeClient client) {
        String versionString = Info.request(client.getInfoPolicyDefault(),
                client.getCluster().getRandomNode(), "version");
        return versionString.substring(versionString.lastIndexOf(' ') + 1);
    }

    private static String clearQualifier(String version) {
        Matcher m = versionPattern.matcher(version);
        if (m.find()) {
            return m.group(0);
        }
        return version;
    }

    private static int compareVersions(String version1, String version2) {
        int comparisonResult = 0;

        String[] version1Splits = version1.split("\\.");
        String[] version2Splits = version2.split("\\.");
        int maxLengthOfVersionSplits = Math.max(version1Splits.length, version2Splits.length);

        for (int i = 0; i < maxLengthOfVersionSplits; i++) {
            Integer v1 = i < version1Splits.length ? Integer.parseInt(version1Splits[i]) : 0;
            Integer v2 = i < version2Splits.length ? Integer.parseInt(version2Splits[i]) : 0;
            int compare = v1.compareTo(v2);
            if (compare != 0) {
                comparisonResult = compare;
                break;
            }
        }
        return comparisonResult;
    }
}
