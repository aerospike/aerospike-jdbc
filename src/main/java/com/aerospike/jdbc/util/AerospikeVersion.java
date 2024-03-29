package com.aerospike.jdbc.util;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;

import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class AerospikeVersion {

    private static final Logger logger = Logger.getLogger(AerospikeVersion.class.getName());

    private static final String S_INDEX_SUPPORT_VERSION = "6.0.0.0";
    private static final String BATCH_OPS_SUPPORT_VERSION = "6.0.0.0";
    private static final String S_INDEX_CARDINALITY_SUPPORT_VERSION = "6.1.0.0";
    private static final Pattern versionPattern = Pattern.compile("^(\\d.){1,3}\\d(?=.*|$)");

    private final IAerospikeClient client;
    private volatile Boolean sIndexSupported;
    private volatile Boolean batchOpsSupported;
    private volatile Boolean sIndexCardinalitySupported;

    public AerospikeVersion(IAerospikeClient client) {
        this.client = client;
    }

    public boolean isSIndexSupported() {
        if (sIndexSupported == null) {
            synchronized (this) {
                if (sIndexSupported == null) {
                    String serverVersion = clearQualifier(getAerospikeServerVersion());
                    sIndexSupported = compareVersions(serverVersion, S_INDEX_SUPPORT_VERSION) >= 0;
                    logger.info(() -> format("Secondary index supported: %b, for version: %s",
                            sIndexSupported, serverVersion));
                }
            }
        }
        return sIndexSupported;
    }

    public boolean isBatchOpsSupported() {
        if (batchOpsSupported == null) {
            synchronized (this) {
                if (batchOpsSupported == null) {
                    String serverVersion = clearQualifier(getAerospikeServerVersion());
                    batchOpsSupported = compareVersions(serverVersion, BATCH_OPS_SUPPORT_VERSION) >= 0;
                    logger.info(() -> format("Batch operations supported: %b, for version: %s",
                            batchOpsSupported, serverVersion));
                }
            }
        }
        return batchOpsSupported;
    }

    public boolean isSIndexCardinalitySupported() {
        if (sIndexCardinalitySupported == null) {
            synchronized (this) {
                if (sIndexCardinalitySupported == null) {
                    String serverVersion = clearQualifier(getAerospikeServerVersion());
                    sIndexCardinalitySupported = compareVersions(serverVersion, S_INDEX_CARDINALITY_SUPPORT_VERSION) >= 0;
                    logger.info(() -> format("Secondary index cardinality supported: %b, for version: %s",
                            sIndexCardinalitySupported, serverVersion));
                }
            }
        }
        return sIndexCardinalitySupported;
    }

    public String getAerospikeServerVersion() {
        String versionString = Info.request(client.getInfoPolicyDefault(),
                client.getCluster().getRandomNode(), "version");
        return versionString.substring(versionString.lastIndexOf(' ') + 1);
    }

    private String clearQualifier(String version) {
        Matcher m = versionPattern.matcher(version);
        if (m.find()) {
            return m.group(0);
        }
        return version;
    }

    private int compareVersions(String version1, String version2) {
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
