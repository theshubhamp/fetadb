### FetaDB
FetaDB is a Work-In-Progress SQL Database backed by a KV store (Badger). It talks the PostgreSQL Wire Protocol but doesn't promise drop-in compatibility.

This is a small attempt to learn database internals!

#### Supported Datatypes
Golang primitive types are supported: bool, string, unit*, int*, float*

#### Supported Column Constraints
Primary Key and Not-Null

#### Supported Features
* In-Memory & Disk Mode. Add option `-dbpath memory` or `-dbpath path/to/dir`
* Non Indexed Table Scan
* Limited support for select, create table, insert into table. For example select does not support where filers

#### Unsupported Features (Current)
* Statements other than select, create table, insert into table
* Scan Filter, Index Scan Filter
* Index Scan, Join, Group By etc.
* Secondary Indexes
* Type Checking on Insert
* Operators other than `=`
* Functions

### Getting Started
#### Install PostgresSQL Client (MacOS)
```shell
brew install libpq
```

#### Run
```shell
go run fetadb
```

#### Connect via Client
```shell
# /usr/local/opt/libpq/bin/psql -h localhost
psql (16.3, server 16.0)
Type "help" for help.

mac=> CREATE TABLE table_name (id uint64 NOT NULL PRIMARY KEY, name string);
--
(0 rows)

mac=> select id, name from table_name;
 res0 | res1
------+------
(0 rows)

mac=> insert into table_name (id, name) values (3, 'Tom Ford');

mac=> insert into table_name (id, name) values (2, 'Jon Doe');

mac=> select id, name from table_name;
 res0 |      res1
------+-----------------
 2    | "Jon Doe"
 3    | "Tom Ford"
(2 rows)

mac=>
mac=> ^D\q
```

### References
- [MyRocks (Facebook's Storage Engine based on RocksDB) KV Encoding](https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format)
- [CockroachDB KV Encoding (New)](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/encoding.md)
- [CockroachDB KV Encoding (Old)](https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/)
- [PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
