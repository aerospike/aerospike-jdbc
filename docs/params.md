# Aerospike JDBC Configuration Parameters

The Aerospike JDBC driver can be configured through the JDBC connection URL. For example:

`jdbc:aerospike:localhost/test?sendKey=true&durableDelete=true&expiration=-1`

The parameters need to match fields of the [Aerospike Java client's](https://javadoc.io/doc/com.aerospike/aerospike-client/latest/index.html)
`Policy`, `ClientPolicy`, `WritePolicy` classes, which take `int` or `boolean`
values.

## Parameters

The optional configuration parameters include the following

| Param | Default | Description | Aerospike Java Client |
|-------|---------|-------------|-----------------------|
| `useBoolBin` | `true` | Use the boolean data type | `Value.UseBoolBin` |
| `compress` | `false` | Use zlib compression on commands to the server | `Policy.compress` |
| `connectTimeout` | 0 | Socket connect timeout in milliseconds | `Policy.connectTimeout` |
| `loginTimeout` | 5000 | Login timeout in milliseconds | `ClientPolicy.loginTimeout` |
| `tendInterval` | 1000 | Interval in milliseconds between cluster tends | `ClientPolicy.tendInterval` |
| `timeout` | 1000 | Cluster tend info call timeout in milliseconds | `ClientPolicy.timeout` |
| `totalTimeout` | 0 | Total transaction timeout in milliseconds | `Policy.totalTimeout` |
| `useServicesAlternate` | `false` | Use "services-alternate" instead of "services" for cluster tending | `ClientPolicy.useServicesAlternate` |
| `sendKey` | `false` | Send user key on both reads and writes | `Policy.sendKey` |
| `durableDelete` | `false` | If the transaction results in a record deletion, leave a tombstone for the record | `Policy.durableDelete` |
| `expiration` | 0 | 0 : use namespace `default-ttl`; -1: never expire; -2 don't change the ttl; otherwise seconds till expiration | `Policy.expiration` |

