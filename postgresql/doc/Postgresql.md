# Postgresql plugin documentation

This plugin allows MLDB to interact with a PostgreSQL database.
For more information about PostgreSQL: <https://www.postgresql.org/>

## Postgresql plugin documentation

Postgresql databases require a username and a password. You can provide
these by registering credentials of type "postgresql" in MLDB. See the credentials 
documentation [Url](Url.md) page.

## PostgreSQL import procedure

This procedure allows to import data from a PostgreSQL database into 
a MLDB dataset.

### Configuration

![](%%config procedure postgresql.import)

## PostgreSQL query function

This function allows to run a single SQL query against a PostgreSQL
database inside a MLDB query.

### Configuration

![](%%config function postgresql.query)

## PostgreSQL recorder dataset

This dataset is write-only and allows rows to be recorded from MLDB 
into a postgreSQL dataset, for instance as the output of a `transform`
procedure.

### Configuration

![](%%config dataset postgresql.recorder)

## PostgreSQL dataset

This dataset is read-only and allows a PostgreSQL dataset table to be
used as a MLDB dataset in a limited fashion.

### Configuration

![](%%config dataset postgresql.recorder)

### WHERE clause

Note that the WHERE clause applied on the PostgreSQL dataset will be 
executed on the PostgreSQL database, as such it must be compatible with 
PostgreSQL and cannot contain any MLDB SQL extensions.

