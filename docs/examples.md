# Aerospike JDBC Supported Statements Examples

The following examples demonstrate CRUD operations in SQL using the Aerospike
JDBC driver.

## Concepts

Let's assume the namespace does not yet have a table (set) named _port_list_ in
the namespace defined in the JDBC URL. There is no need to repeat the database
(namespace) when it's provided in the connection string.

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
the Aerospike record's _digest_ (see the
[Glossary](https://www.aerospike.com/docs/guide/glossary.html)). The Aerospike
JDBC driver always sends the primary key.

Notice that this table is shown even though it does not explicitly exist.
That is because Aerospike is schemaless, with tables (Aerospike _sets_)
created upon insertion of a new row.

For more on the [Aerospike data model](https://www.aerospike.com/docs/architecture/data-model.html)
see the Aerospike documentation.

### Table and column naming
You should be aware of the [known limitations](https://docs.aerospike.com/guide/limitations)
on table (set) and column (bin) names in Aerospike.

> Names can include only Latin lowercase and uppercase letters with no diacritical marks (a-z, A-Z), digits 0-9, underscores (_), hyphens (-), and dollar signs ($). This naming guideline is not enforced; however, if you do not follow it, some Aerospike features and tools might not function properly.

Table names are limited to 63 characters and column names to 15 characters.

In the JDBC driver it is recommended that you use quotes around table names. For
example

```sql
SELECT * FROM "top-users";
```

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
```

Multiple rows can be inserted at once:
```sql
INSERT INTO port_list (__key, port, description) VALUES ("memcache", 11211, "Memcached"), ("battlefield2", 16567, "Battlefield 2");
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

Batch query for rows in a list of primary keys:

```sql
SELECT * FROM port_list WHERE __key IN ("ntp", "ror");
```

__key|description                                        |extra|port|
-----|---------------------------------------------------|-----|----|
ror  |Ruby on Rails development default                  |     |3000|
ntp  |Network Time Protocol used for time synchronization|     | 123|


### Multiple predicates
Query for rows that satisfy a `WHERE` with more than one predicate

```sql
SELECT * FROM port_list WHERE description="Battlefield 2" AND port=16567;
```

__key       |description  |port |
------------|-------------|-----|
battlefield2|Battlefield 2|16567|

```sql
SELECT * FROM port_list WHERE port=123 OR port=161;
```

__key |description                                        |port |
------|---------------------------------------------------|-----|
snmp  |Simple Network Management Protocol (SNMP)          | 161 |
ntp   |Network Time Protocol used for time synchronization| 123 |

```sql
SELECT * FROM port_list WHERE port=3000 AND NOT(description="Aerospike Database");
```

__key    |description                             |port|
---------|----------------------------------------|----|
cloud9ide|Cloud9 IDE Server                       |3000|
dis      |Distributed Interactive Simulation (DIS)|3000|
ror      |Ruby on Rails development default       |3000|

Range queries are done using `BETWEEN`.

```sql
SELECT * FROM port_list WHERE port BETWEEN 100 AND 200;
```

__key   |description                                        |port|
--------|---------------------------------------------------|----|
snmp    |Simple Network Management Protocol (SNMP)          | 161|
snmptrap|Simple Network Management Protocol Trap(SNMPTRAP)  | 162|
ntp     |Network Time Protocol used for time synchronization| 123|

### Secondary Indexes

[Secondary indexes](https://docs.aerospike.com/server/guide/queries)
can be [optionally added](https://docs.aerospike.com/tools/asadm/user_guide/live_cluster_mode_guide#secondary-indexes-1)
to accelerate `BETWEEN` range queries on integer values or equality predicates
on integer or string values. The JDBC driver will create an SI query if a
secondary index is available.

You can use asadm to [add a secondary index](https://docs.aerospike.com/tools/asadm/user_guide/live_cluster_mode_guide#secondary-indexes-1).
```
Admin> enable
Admin+> manage sindex create numeric port-idx ns test set port_list bin port
Admin+> show sindex
~~~~~~Secondary Indexes (2022-05-17 07:12:58 UTC)~~~~~~
   Index|Namespace|      Set| Bin|    Bin|  Index|State
    Name|         |         |    |   Type|   Type|     
port-idx|test     |port_list|port|NUMERIC|DEFAULT|RW  
```

## Aggregate functions
Count the records in the table that don't use port 3000:

```sql
SELECT COUNT(*) FROM port_list WHERE port <> 3000;
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

Aerospike columns (bins) can be dropped by assigning a `NULL` to them.

```sql
UPDATE port_list SET extra=NULL;
SELECT * FROM port_list WHERE port < 200;
```

__key   |description                                        |port|
--------|---------------------------------------------------|----|
ntp     |Network Time Protocol used for time synchronization| 123|
snmp    |Simple Network Management Protocol (SNMP)          | 161|
snmptrap|Simple Network Management Protocol Trap(SNMPTRAP)  | 162|

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

## CREATE INDEX

```sql
CREATE INDEX port_idx ON port_list (port);
```

## DROP INDEX

```sql
DROP INDEX port_idx ON port_list;
```

## Transactions
**Note:** Wrapping multiple commands in a transaction requires Aerospike Database version 8.0+.

[JDBC transactions](https://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html) are started by setting auto-commit to false, which acts as an implicit `BEGIN`. Every subsequent command is part of the transaction until a
commit or rollback are issued. A new transaction begins automatically after either is executed. Switching back to auto-commit will rollback an uncommitted transaction.

In a data browser like DBeaver, the UI has buttons to control switching the auto-commit on and off, along with commit and rollback buttons.

```sql
-- Switch to manual (begins the transaction)
SELECT * FROM port_list WHERE __key="ntp";
UPDATE port_list SET port=124 where __key="ntp";
UPDATE port_list SET port=162 where __key="snmp";
--COMMIT

--Switch to auto
SELECT * FROM port_list WHERE __key IN ("ntp", "snmp");

-- Switch to manual (begins the transaction)
UPDATE port_list SET port=123 where __key="ntp";
UPDATE port_list SET port=161 where __key="snmp";
--Rollback

-- Switch to auto
SELECT * FROM port_list WHERE __key IN ("ntp", "snmp");

-- Switch to manual (begins the transaction)
UPDATE port_list SET port=123 where __key="ntp";
UPDATE port_list SET port=161 where __key="snmp";
--Commit

-- Switch to auto
SELECT * FROM port_list WHERE __key IN ("ntp", "snmp");
```
