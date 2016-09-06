# MongoDB Query function

The MondoDB query function allows the creation of a function to perform an
MLDB [SQL query](/doc/builtin/sql/Sql.md) against a MongoDB collection. It acts
similarly to the
[SQL Query function](/doc/builtin/functions/SqlQueryFunction.md).

## Caveat
- mongodb.query will not work on records having key "_id" that are not
  ObjectIDs.

## Configuration

![](%%config function mongodb.query)
