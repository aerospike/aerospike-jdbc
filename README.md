# Aerospike JDBC Driver

Aerospike JDBC Driver allows you to interact with Aerospike clusters by using SQL statements from your Java application.  
Read [Java Tutorials](https://docs.oracle.com/javase/tutorial/jdbc/basics/index.html) to get started with JDBC.

## Prerequisites
* Java 8 or later
* Aerospike Server version 5.2+

## Build
```sh
mvn clean package
```
The JDBC driver jar `uber-aerospike-jdbc-driver-<version>.jar` will be created under the target folder.

Pre-built versions of the driver are available in the [Releases](https://github.com/aerospike/aerospike-jdbc/releases).

## JDBC connection properties
|     |     |
| --- | --- |
| JDBC Driver | `com.aerospike.jdbc.AerospikeDriver` |
| JDBC URL | `jdbc:aerospike:HOST[:PORT][/NAMESPACE][?PARAM1=VALUE1[&PARAM2=VALUE2]`<sup>[1](#jdbc-url)</sup> |
<sup name="jdc-url">1</sup> For example `jdbc:aerospike:localhost` connects to the Aerospike database running on a local machine and listening on the default port (3000).
The `jdbc:aerospike:172.17.0.5:3300/test` URL connects to the `test` namespace on the Aerospike database running on `172.17.0.5:3300`.

## Usage example
```java
try {
    String url = "jdbc:aerospike:localhost:3300/test";
    Connection connection = DriverManager.getConnection(url);

    String query = "select * from ns1 limit 10";
    ResultSet resultSet = connection.createStatement().executeQuery(query);
    while (resultSet.next()) {
        String bin1 = resultSet.getString("bin1");
        System.out.println(bin1);
    }
} catch (Exception e) {
    System.err.println(e.getMessage());
}
```

## Supported SQL Statements
* SELECT
* INSERT
* UPDATE
* DELETE

<sup>1</sup> JOIN, nested SELECT and GROUP BY statements are not in the scope of the current version.

<sup>2</sup> The development is in progress, and minor features documentation is not maintained now.

## JDBC Client tools
* [DBeaver](https://dbeaver.io/)  
    Configure the Aerospike JDBC Driver:  
    * Database -> Driver Manager -> New  
    Fill in settings:
        * Driver Name: Aerospike
        * Driver Type: Generic
        * Class Name: `com.aerospike.jdbc.AerospikeDriver`
        * URL Template: `jdbc:aerospike:{host}[:{port}]/[{database}]`
        * Default Port: 3000
    * Click the `Add File` button and add the JDBC jar file.
    * Click the `Find Class` button.
    * Click `OK`.
    
    Create a connection:  
    * Database -> New Database Connection
    * Select `Aerospike` and click `Next`.
    * Fill in the connection settings and click `Finish`.
    
* [SQuirreL](http://squirrel-sql.sourceforge.net/)

## License
Licensed under the Apache 2.0 License.