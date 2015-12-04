# Sub Dataset

The Sub Dataset temporarily stores the result of an SQL statement in order to be used 
in a parent FROM statement.

For example:

'SELECT * FROM (SELECT * FROM dataset WHERE column1 = 2) WHERE column2 = 4'

It is used during query execution and not meant to be explicitely created by users.

## Configuration

![](%%config dataset sub)

