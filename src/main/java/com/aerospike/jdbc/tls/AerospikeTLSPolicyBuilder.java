package com.aerospike.jdbc.tls;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.TlsPolicy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;

public class AerospikeTLSPolicyBuilder {

    private static final Logger log = Logger.getLogger(AerospikeTLSPolicyBuilder.class.getName());

    private final AerospikeTLSPolicyConfig tlsPolicyConfig;

    public AerospikeTLSPolicyBuilder(AerospikeTLSPolicyConfig tlsPolicyConfig) {
        this.tlsPolicyConfig = tlsPolicyConfig;
    }

    public TlsPolicy build() {
        if (!tlsPolicyConfig.getEnabled()) {
            return null;
        }

        log.info("Init TlsPolicy");
        TlsPolicy policy = new TlsPolicy();
        if (tlsPolicyConfig.getKeystorePath() != null || tlsPolicyConfig.getTruststorePath() != null) {
            addSSLContext(policy);
        }
        if (tlsPolicyConfig.getAllowedCiphers() != null) {
            policy.ciphers = tlsPolicyConfig.getAllowedCiphers();
        }
        if (tlsPolicyConfig.getAllowedProtocols() != null) {
            policy.protocols = tlsPolicyConfig.getAllowedProtocols();
        }
        if (tlsPolicyConfig.getForLoginOnly() != null) {
            policy.forLoginOnly = tlsPolicyConfig.getForLoginOnly();
        }
        return policy;
    }

    private void addSSLContext(TlsPolicy tlsPolicy) {
        tlsPolicy.context = getSSLContext();
    }

    private SSLContext getSSLContext() {
        SSLContextBuilder ctxBuilder = SSLContexts.custom();
        ctxBuilder.setKeyStoreType(tlsPolicyConfig.getStoreType());
        if (tlsPolicyConfig.getKeystorePath() != null) {
            loadKeyStore(ctxBuilder);
        }
        if (tlsPolicyConfig.getTruststorePath() != null) {
            loadTrustStore(ctxBuilder);
        }

        try {
            return ctxBuilder.build();
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            throw new AerospikeException("Failed to build SSLContext", e);
        }
    }

    private void loadTrustStore(SSLContextBuilder ctxBuilder) {
        File tsFile = new File(tlsPolicyConfig.getTruststorePath());
        try {
            if (tlsPolicyConfig.getTruststorePassword() != null) {
                ctxBuilder.loadTrustMaterial(tsFile, tlsPolicyConfig.getTruststorePassword().toCharArray());
            } else {
                ctxBuilder.loadTrustMaterial(tsFile);
            }
        } catch (NoSuchAlgorithmException | KeyStoreException | CertificateException | IOException e) {
            throw new AerospikeException("Failed To load truststore", e);
        }
    }

    private void loadKeyStore(SSLContextBuilder ctxBuilder) {
        requireNonNull(tlsPolicyConfig.getKeystorePassword(),
                "If Keystore Path is provided, Keystore Password must be provided");

        File ksFile = new File(tlsPolicyConfig.getKeystorePath());
        try {
            if (tlsPolicyConfig.getKeyPassword() == null) {
                // If keyPass is not provided, assume it is the same as the keystore password
                ctxBuilder.loadKeyMaterial(ksFile, tlsPolicyConfig.getKeystorePassword().toCharArray(),
                        tlsPolicyConfig.getKeystorePassword().toCharArray());
            } else {
                ctxBuilder.loadKeyMaterial(ksFile, tlsPolicyConfig.getKeystorePassword().toCharArray(),
                        tlsPolicyConfig.getKeyPassword().toCharArray());
            }
        } catch (NoSuchAlgorithmException | KeyStoreException | CertificateException | UnrecoverableKeyException | IOException e) {
            throw new AerospikeException("Failed To load keystore", e);
        }
    }
}
