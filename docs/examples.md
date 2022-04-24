# Aerospike JDBC Supported Statements Examples

The following examples demonstrate CRUD operations in SQL using the Aerospike
JDBC driver.

## Concepts

Let's assume the namespace does not have a table named _port_list_.

```sql
SELECT * FROM port_list;
```

| __key<sup>[1](#key)</sup> |
| ------------------------- |
|         &nbsp;            |

<sup name="key">1</sup> Aerospike always has a primary key index
on an identifier that isn't one of the bins (columns).

To represent this in SQL, `__key` is a magic column. When developers use
`sendKey=true` in their applications, the `__key` column will
reflect the _userKey_ that is saved with any write operation. Otherwise, it
will appear to be NULL, but each row still has a distinct object identifier,
the Aerospike record's _diget_ (see the
[Glossary](https://www.aerospike.com/docs/guide/glossary.html)). The Aerospike
JDBC driver always sends the primary key.

Notice that this table is shown even though it does not explicitly exist.
That is because Aerospike is schemaless, with tables (Aerospike _sets_)
created upon insertion of a new row.

For more on the [Aerospike data model](https://www.aerospike.com/docs/architecture/data-model.html)
see the Aerospike documentation.

## INSERT

Let's add some rows with explicit primary keys:

```sql
INSERT INTO port_list (__key, port, description) VALUES ("ntp", 123, "Network Time Protocol used for time synchronization");
INSERT INTO port_list (__key, port, description) VALUES ("snmp", 161, "Simple Network Management Protocol (SNMP)");
INSERT INTO port_list (__key, port, description) VALUES ("snmptrap", 162, "Simple Network Management Protocol Trap(SNMPTRAP)");
INSERT INTO port_list (__key, port, description) VALUES ("aerospike", 3000, "Aerospike Database");
INSERT INTO port_list (__key, port, description) VALUES ("cloud9ide", 3000, "Cloud9 IDE Server");
INSERT INTO port_list (__key, port, description) VALUES ("ror", 3000, "Ruby on Rails development default");
INSERT INTO port_list (__key, port, description) VALUES ("dis", 3000, "Distributed Interactive Simulation (DIS)");
INSERT INTO port_list (__key, port, description) VALUES ("fcip", 3225, "Fibre Channel over IP (FCIP)");
INSERT INTO port_list (__key, port, description) VALUES ("metasys", 11001, "Johnson Controls Metasys java AC control environment");
INSERT INTO port_list (__key, port, description) VALUES ("memcache", 11211, "Memcached");
INSERT INTO port_list (__key, port, description) VALUES ("battlefield2", 16567, "Battlefield 2");
```

As an Aerospike row (_record_) must have a primary key, if none is provided
the JDBC driver will generate a random UUID for it.

```sql
INSERT INTO port_list (port, description) VALUES (47, NULL);
```

## SELECT

A simple query over the rows in the table:

```sql
SELECT * FROM port_list WHERE port < 100;
```

__key                               |description|port|
------------------------------------|-----------|----|
05511f2b-4ace-4fc3-93b6-7053a6fe5d8c|           |  47|

A simple SELECT query with a limit on the number of rows in the result:

```sql
SELECT * FROM port_list WHERE description IS NOT NULL LIMIT 5;
```

Will result in:

__key       |description                                        |port |
------------|---------------------------------------------------|-----|
snmp        |Simple Network Management Protocol (SNMP)          |  161|
ntp         |Network Time Protocol used for time synchronization|  123|
aerospike   |Aerospike Database                                 | 3000|
battlefield2|Battlefield 2                                      |16567|
cloud9ide   |Cloud9 IDE Server                                  | 3000|

Query for a specific row using its primary key:

```sql
SELECT * FROM port_list WHERE __key="memcache";
```

__key   |description|port |
--------|-----------|-----|
memcache|Memcached  |11211|

Query for rows that satisfy a `WHERE` with more than one predicate

```sql
SELECT * FROM port_list WHERE description="Battlefield 2" AND port=16567;
```

__key       |description  |port |
------------+-------------+-----+
battlefield2|Battlefield 2|16567|

Count the records in the table that don't use port 3000:

```sql
SELECT COUNT(*) FROM port_list WHERE port != 3000;
```

COUNT(*)|
--------|
       7|

## UPDATE
Update a row using its primary key:

```sql
UPDATE port_list SET description="Battlefield 2 and mods" WHERE __key="battlefield2";
SELECT * FROM port_list WHERE __key="battlefield2";
```

__key       |description           |port |
------------|----------------------|-----|
battlefield2|Battlefield 2 and mods|16567|

A simple update statement using a non-PK predicate:

```sql
UPDATE port_list SET description="Reserved" WHERE description IS NULL;
SELECT * FROM port_list WHERE port = 47;
```

__key                               |description|port|
------------------------------------|-----------|----|
05511f2b-4ace-4fc3-93b6-7053a6fe5d8c|Reserved   |  47|

A column (bin) can be added to the table using an update statement without a
`WHERE` clause.

```sql
UPDATE port_list SET extra=1;
SELECT * FROM port_list WHERE port <> 47;
```

__key       |description                                         |extra|port |
------------|----------------------------------------------------|-----|-----|
snmp        |Simple Network Management Protocol (SNMP)           |    1|  161|
ntp         |Network Time Protocol used for time synchronization |    1|  123|
aerospike   |Aerospike Database                                  |    1| 3000|
battlefield2|Battlefield 2 and mods                              |    1|16567|
cloud9ide   |Cloud9 IDE Server                                   |    1| 3000|
dis         |Distributed Interactive Simulation (DIS)            |    1| 3000|
metasys     |Johnson Controls Metasys java AC control environment|    1|11001|
fcip        |Fibre Channel over IP (FCIP)                        |    1| 3225|
memcache    |Memcached                                           |    1|11211|
ror         |Ruby on Rails development default                   |    1| 3000|
snmptrap    |Simple Network Management Protocol Trap(SNMPTRAP)   |    1|  162|

Since Aerospike is schemaless, the data browser may need to be refreshed for it
to pick up the new _extra_ column.

## DELETE

Delete rows that match a WHERE condition on a regular column (a bin):

```sql
DELETE FROM port_list WHERE port > 200;
SELECT * FROM port_list;
```

__key                               |description                                        |port|
------------------------------------|---------------------------------------------------|----|
05511f2b-4ace-4fc3-93b6-7053a6fe5d8c|Reserved                                           |  47|
snmp                                |Simple Network Management Protocol (SNMP)          | 161|
ntp                                 |Network Time Protocol used for time synchronization| 123|
snmptrap                            |Simple Network Management Protocol Trap(SNMPTRAP)  | 162|

Delete a row by its primary key (the __key column).

```sql
DELETE FROM port_list WHERE __key="snmp";
SELECT COUNT(*) FROM port_list;
```

|COUNT(*)|
|--------|
|       3|


To delete all the rows in a table:

```sql
DELETE FROM port_list;
SELECT COUNT(*) FROM port_list;
```

|COUNT(*)|
|--------|
|       0|


## TRUNCATE

The `TRUNCATE TABLE` command is equivalent to a `DELETE` without a `WHERE`
clause.

```sql
TRUNCATE TABLE port_list;

-- is the same as

DELETE FROM port_list;
```

