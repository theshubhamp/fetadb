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
* Operator dispatch, supported `=`, `+`, `-`, `*`, `/` and `||`
* Functions dispatch, supported `lower`, `upper`, `md5`
* Sort (In-Memory)

#### Unsupported Features (Current)
* Statements other than select, create table, insert into table
* Scan Filter, Index Scan Filter
* Index Scan, Join, Group By etc.
* Secondary Indexes
* Type Checking on Insert

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

mac=> CREATE TABLE Departments(DepartmentID uint64 PRIMARY KEY, DepartmentName string NOT NULL);

mac=> CREATE TABLE Employees(EmployeeID uint64 PRIMARY KEY, FirstName string NOT NULL, LastName string NOT NULL, DepartmentID uint64, Salary float64);

mac=> INSERT INTO Departments (DepartmentID, DepartmentName) 
      VALUES (1, 'HR'), (2, 'IT'), (3, 'Finance'), (4, 'Marketing'), (5, 'Operations');
      
mac=> INSERT INTO Employees (EmployeeID, FirstName, LastName, DepartmentID, Salary)
      VALUES  (1, 'John', 'Doe', 1, 60000),
              (2, 'Jane', 'Smith', 2, 75000),
              (3, 'Mike', 'Johnson', 3, 65000),
              (4, 'Emily', 'Brown', 2, 72000),
              (5, 'David', 'Lee', 4, 68000),
              (6, 'Sarah', 'Wilson', 1, 62000),
              (7, 'Tom', 'Davis', 5, 55000),
              (8, 'Anna', 'Taylor', 3, 70000),
              (9, 'Chris', 'Anderson', 5, 58000),
              (10, 'Lisa', 'Thomas', 5, 59000);

mac=> select EmployeeID, FirstName, LastName, DepartmentID, Salary from Employees order by departmentid desc, salary asc;
 employeeid | firstname |  lastname  | departmentid | salary
------------+-----------+------------+--------------+--------
 9          | "Chris"   | "Anderson" | 5            | 58000
 5          | "David"   | "Lee"      | 4            | 68000
 3          | "Mike"    | "Johnson"  | 3            | 65000
 8          | "Anna"    | "Taylor"   | 3            | 70000
 10         | "Lisa"    | "Thomas"   | 2            | 59000
 4          | "Emily"   | "Brown"    | 2            | 72000
 2          | "Jane"    | "Smith"    | 2            | 75000
 7          | "Tom"     | "Davis"    | 1            | 55000
 1          | "John"    | "Doe"      | 1            | 60000
 6          | "Sarah"   | "Wilson"   | 1            | 62000
(10 rows)

```

### References
- [MyRocks (Facebook's Storage Engine based on RocksDB) KV Encoding](https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format)
- [CockroachDB KV Encoding (New)](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/encoding.md)
- [CockroachDB KV Encoding (Old)](https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/)
- [PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
