# SQLite Sparse Dataset

The SQLite Sparse Dataset is a [dataset] (Datasets.md) that stores its data
on local files inside the MLDB container using the popular [SQLite3 database]
(http://www.sqlite.org).

This dataset has the following characteristics:

- It is durable, in other words the data is kept on disk
- It is mutable, on other words it is possible to continually record to the
  dataset.
- Fast startup time.  The dataset loads nearly instantaneously, and is only
  read as data is accessed.
- It models a sparse matrix, not a table, and so is suitable for storing
  very sparse datasets that are often used by MLDB.
- It models timestamps down to a one millisecond resolution.

The main disadvantage of the SQLite Sparse Dataset is that it is slow to
record data in to it and it uses quite a bit of disk space.  Typically, no
more than a few thousand rows can be recorded per second (although this may
be higher on systems with `mldb_data` mounted on an SSD) and the size of the
file on disk will be significantly larger than a text file containing the
data inserted.

It is used internally in MLDB to store internal operational
data.

This dataset cannot be used to access or manipulate tables in SQLite
databases that it did not create.

## Configuration

![](%%config dataset sqliteSparse)

The `dataFileUrl` parameter *must* be a `file://` URL, as SQLite only works
with local files.  If the parameter is empty, then a temporary, in-memory
database will be used that is *not* persisted to disk.

# See Also

* [SQLite3 database] (http://www.sqlite.org)

# Appendix - Data Schema

The database includes three tables: `vals`, `rows` and `cols`.

The `rows` table has three columns: `rowNum`, `rowHash` and `rowName`.  Each
row in this table describes a row in the sparse dataset.

The `cols` table has three columns: `colNum`, `colHash` and `colName`.  Each
row in this table describes a column in the sparse dataset.

The `vals` table contains the actual data inserted into the table.  Each value
recorded is a separate row.  The columns are `rowNum` (a small integer giving
the index into the `rows` table), `colNum` (a small integer giving the index
into the `cols` table), `val` (a JSON-encoded string of the value in the cell)
and `ts` (timestamp, the integral number of milliseconds since the eopch).

The `rows` and `cols` tables exist as an optimization, to avoid recording the
same row and column names over and over.
