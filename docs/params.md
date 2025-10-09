# Aerospike JDBC Configuration Parameters

The Aerospike JDBC driver can be configured through the JDBC connection URL. For example:

`jdbc:aerospike:localhost/test?sendKey=true&durableDelete=true&expiration=-1`

For detailed information about configuration properties, see the following sections.

## Aerospike Java client configuration

The parameters need to match fields of the [Aerospike Java client's](https://javadoc.io/doc/com.aerospike/aerospike-client/latest/index.html)
`Policy` and its subclasses, which take `String`, `int` or `boolean` values.

The optional configuration parameters include the following

| Param                | Default | Description                                                                                                   | Aerospike Java Client               |
|----------------------|---------|---------------------------------------------------------------------------------------------------------------|-------------------------------------|
| useBoolBin           | `true`  | Use the boolean data type                                                                                     | `Value.UseBoolBin`                  |
| compress             | `false` | Use zlib compression on commands to the server                                                                | `Policy.compress`                   |
| connectTimeout       | 0       | Socket connect timeout in milliseconds                                                                        | `Policy.connectTimeout`             |
| loginTimeout         | 5000    | Login timeout in milliseconds                                                                                 | `ClientPolicy.loginTimeout`         |
| tendInterval         | 1000    | Interval in milliseconds between cluster tends                                                                | `ClientPolicy.tendInterval`         |
| timeout              | 1000    | Cluster tend info call timeout in milliseconds                                                                | `ClientPolicy.timeout`              |
| totalTimeout         | 0       | Total transaction timeout in milliseconds                                                                     | `Policy.totalTimeout`               |
| useServicesAlternate | `false` | Use "services-alternate" instead of "services" for cluster tending                                            | `ClientPolicy.useServicesAlternate` |
| sendKey              | `false` | Send user key on both reads and writes                                                                        | `Policy.sendKey`                    |
| durableDelete        | `false` | If the transaction results in a record deletion, leave a tombstone for the record                             | `Policy.durableDelete`              |
| expiration           | 0       | 0 : use namespace `default-ttl`; -1: never expire; -2 don't change the ttl; otherwise seconds till expiration | `Policy.expiration`                 |

## JDBC Driver configuration

The following parameters configure the internal state of the driver.
Their default values are sufficient in most cases.
Consider setting a custom value if really necessary.

| Param                   | Default | Description                                                            |
|-------------------------|---------|------------------------------------------------------------------------|
| recordSetQueueCapacity  | 256     | The capacity of the record queue for asynchronous Aerospike operations |
| recordSetTimeoutMs      | 1000    | Timeout for the asynchronous queue write operation in milliseconds     |
| metadataCacheTtlSeconds | 3600    | Database metadata cache TTL in seconds                                 |
| schemaBuilderMaxRecords | 1000    | The number of records to be used to build the table schema             |
| showRecordMetadata      | false   | Add record metadata columns (__digest, __ttl, __gen)                   |
| txnTimeoutSeconds       | 10      | Multi-record transaction timeout in seconds                            |
| refuseScan              | false   | Forbid queries without an available secondary index                    |
| queryLimit              | 0       | Apply default record limit to queries                                  |
