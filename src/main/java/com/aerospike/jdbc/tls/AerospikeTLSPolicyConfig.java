package com.aerospike.jdbc.tls;

import java.util.Objects;
import java.util.Properties;

public class AerospikeTLSPolicyConfig {

    private static final String TLS_ENABLED = "tlsEnabled";
    private static final String TLS_STORETYPE = "tlsStoreType";
    private static final String TLS_KEYSTOREPATH = "tlsKeystorePath";
    private static final String TLS_KEYSTOREPASSWORD = "tlsKeystorePassword";
    private static final String TLS_KEYPASSWORD = "tlsKeyPassword";
    private static final String TLS_TRUSTSTOREPATH = "tlsTruststorePath";
    private static final String TLS_TRUSTSTOREPASSWORD = "tlsTruststorePassword";
    private static final String TLS_FORLOGINONLY = "tlsForLoginOnly";
    private static final String TLS_ALLOWEDCIPHERS = "tlsAllowedCiphers";
    private static final String TLS_ALLOWEDPROTOCOLS = "tlsAllowedProtocols";

    private boolean enabled;
    private String storeType = "jks";
    private String keystorePath;
    private String keystorePassword;
    private String keyPassword;
    private String truststorePath;
    private String truststorePassword;
    private Boolean forLoginOnly;
    private String[] allowedCiphers;
    private String[] allowedProtocols;

    public static AerospikeTLSPolicyConfig fromProperties(Properties props) {
        AerospikeTLSPolicyConfig config = new AerospikeTLSPolicyConfig();
        props.forEach((key, value) -> {
            String propKey = (String) key;
            switch (propKey) {
                case TLS_ENABLED:
                    config.setEnabled(Boolean.parseBoolean(value.toString()));
                    break;
                case TLS_STORETYPE:
                    config.setStoreType(value.toString());
                    break;
                case TLS_KEYSTOREPATH:
                    config.setKeystorePath(value.toString());
                    break;
                case TLS_KEYSTOREPASSWORD:
                    config.setKeystorePassword(value.toString());
                    break;
                case TLS_KEYPASSWORD:
                    config.setKeyPassword(value.toString());
                    break;
                case TLS_TRUSTSTOREPATH:
                    config.setTruststorePath(value.toString());
                    break;
                case TLS_TRUSTSTOREPASSWORD:
                    config.setTruststorePassword(value.toString());
                    break;
                case TLS_FORLOGINONLY:
                    config.setForLoginOnly(Boolean.parseBoolean(value.toString()));
                    break;
                case TLS_ALLOWEDCIPHERS:
                    config.setAllowedCiphers(value.toString());
                    break;
                case TLS_ALLOWEDPROTOCOLS:
                    config.setAllowedProtocols(value.toString());
                    break;
                default:
                    break;
            }
        });
        return config;
    }

    public boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getStoreType() {
        return storeType;
    }

    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    public String getKeystorePath() {
        return keystorePath;
    }

    public void setKeystorePath(String keystorePath) {
        this.keystorePath = keystorePath;
    }

    public String getKeystorePassword() {
        return keystorePassword;
    }

    public void setKeystorePassword(String keystorePassword) {
        this.keystorePassword = keystorePassword;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    public String getTruststorePath() {
        return truststorePath;
    }

    public void setTruststorePath(String truststorePath) {
        this.truststorePath = truststorePath;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    public Boolean getForLoginOnly() {
        return forLoginOnly;
    }

    public void setForLoginOnly(Boolean forLoginOnly) {
        this.forLoginOnly = forLoginOnly;
    }

    public String[] getAllowedCiphers() {
        return allowedCiphers;
    }

    public void setAllowedCiphers(String allowedCiphers) {
        if (Objects.nonNull(allowedCiphers)) {
            this.allowedCiphers = allowedCiphers.trim().split(",");
        }
    }

    public String[] getAllowedProtocols() {
        return allowedProtocols;
    }

    public void setAllowedProtocols(String allowedProtocols) {
        if (Objects.nonNull(allowedProtocols)) {
            this.allowedProtocols = allowedProtocols.trim().split(",");
        }
    }
}
