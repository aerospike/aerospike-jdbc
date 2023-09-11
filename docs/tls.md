# Aerospike JDBC TLS Configuration Parameters

The Aerospike JDBC driver can be configured to use SSL through the JDBC connection URL. 

Below are the tls-related configs for aerospike-jdbc (example with value):
```tlsEnabled true
tlsName TLS_NAME
tlsStoreType (like jks)
tlsTruststorePassword   ******* (if required)
tlsTruststorePath  ./../XXX.jks (the full path)
```
The full list of the valid values can be found at [AerospikeTLSPolicyConfig](https://github.com/aerospike/aerospike-jdbc/blob/main/src/main/java/com/aerospike/jdbc/tls/AerospikeTLSPolicyConfig.java).

The configuration expects a standard Java truststore. An example of adding a CA certificate to the Java Truststore can be found at [Add CA certificate to Java TrustStore on client nodes](https://docs.aerospike.com/server/operations/configure/network/tls/mtls_java#add-ca-certificate-to-java-truststore-on-client-nodes).

```keytool -import -alias tls1 -file /etc/aerospike/ssl/tls1/cert.pem -keystore //usr/lib/jvm/java-8-openjdk-amd64/jre/lib/security/cacerts -storePass changeit
```
Here are some working URL example using the above configs:

```//aerospike server detail
//host:172.17.0.9 port:4333 namespace:test tlsname:tls1 (as per the CN, same used in the truststore alias as well)

//connection URL example1
"jdbc:aerospike:172.17.0.9:4333/test?tlsEnabled=true&tlsName=tls1&tlsTruststorePath=/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/security/cacerts&tlsTruststorePassword=changeit";

//connection URL example2
"jdbc:aerospike:172.17.0.9:4333/test?tlsEnabled=true&tlsName=tls1&tlsStoreType=jks&tlsTruststorePath=/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/security/cacerts&tlsTruststorePassword=changeit";
```
