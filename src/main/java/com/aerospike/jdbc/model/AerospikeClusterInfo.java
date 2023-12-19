package com.aerospike.jdbc.model;

import java.util.Collection;
import java.util.Map;

public class AerospikeClusterInfo {

    private final String build;
    private final String edition;
    private final Collection<String> catalogs;
    private final Map<String, Collection<String>> tables;

    public AerospikeClusterInfo(String build, String edition, Collection<String> catalogs,
                                Map<String, Collection<String>> tables) {
        this.build = build;
        this.edition = edition;
        this.catalogs = catalogs;
        this.tables = tables;
    }

    public String getBuild() {
        return build;
    }

    public String getEdition() {
        return edition;
    }

    public Collection<String> getCatalogs() {
        return catalogs;
    }

    public Map<String, Collection<String>> getTables() {
        return tables;
    }
}
