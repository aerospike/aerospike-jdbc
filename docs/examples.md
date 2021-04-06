# Aerospike JDBC Supported Statements Examples

The below query examples will have been run on a table (set) named `table1` with the following columns (bin names):

| table1 |
| ------ |
| __key<sup>[1](#key)</sup> |
| bin1   |
| id     |
| int1   |
| list   |
| map    |

<sup name="key">1</sup> Aerospike always has a primary key on an identifier that isn't one of the bins (columns).
To represent this in SQL, `__key` is a magic column. Developers should use the `WritePolicy.sendKey=true` writing to Aerospike.
Otherwise, it will appear to be NULL.

## INSERT
INSERT query specifying the primary key.
```sql
insert into table1 (__key, bin1, "int") values (abc, 11101, 3)
```

If no primary key is provided, the random UUID will be generated for it.
```sql
insert into table1 (bin1, "int1") values (11101, 3)
```

And now, select-query the inserted row.
```sql
select * from table1 where bin1>10000
```
Will result in:

| __key | bin1 | id | int1 | list | map |
| --- | --- | --- | --- | --- | --- |
| 702daeb1-4904-4973-bda5-70f4999ee627 | 11101 | NULL | 3 | NULL | NULL |

## SELECT
A simple SELECT query.
```sql
select * from table1 limit 10
```

Will result in:

| __key  | bin1 | id    | int1       | list   | map      |
| ------ | ---- | ----- | ---------- | ------ | -------- | 
| key_92 | NULL | id_92 | 1508976092 | NULL   | [{k1=1}] |
| key_74 | NULL | id_74 | 1508976074 | NULL   | NULL     |
| key_12 | NULL | id_12 | 1508976012 | str_12 | [{k1=1}] |
| key_62 | NULL | id_62 | 1508976062 | NULL   | NULL     |
| key_15 | 15   | id_15 |       NULL | str_15 | NULL     |
| key_26 | NULL | id_26 | 1508976026 | NULL   | NULL     |
| key_46 | NULL | id_46 | 1508976046 | NULL   | NULL     |
| key_81 | 81   | id_81 |       NULL | str_81 | NULL     |
| key_87 | 87   | id_87 |       NULL | str_87 | NULL     |
| key_9  |  9   | id_9  |       NULL | str_9  | NULL     |

Select using a primary key.
```sql
select * from table1 where __key="key_92"
```

| __key  | bin1 | id    | int1       | list   | map      |
| ------ | ---- | ----- | ---------- | ------ | -------- |    
| key_92 | NULL | id_92 | 1508976092 | NULL   | [{k1=1}] |

Select count query.
```sql
select count(*) from table1
```

100

## UPDATE
Update a row using a primary key.
```sql
update table1 set bin1=1 where __key="702daeb1-4904-4973-bda5-70f4999ee627"
```

A simple update statement using a WHERE clause.
```sql
update table1 set bin1=1 where bin1 is null
```

## DELETE
To delete the entire table.
```sql
delete from table1
```

Delete the inserted row by the primary key (the __key column).
```sql
delete from table1 where __key="702daeb1-4904-4973-bda5-70f4999ee627"
```

Delete the inserted row using WHERE condition on a regular column.
```sql
delete from table1 where bin1>10000
```
