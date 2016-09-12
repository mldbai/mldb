# MongoDB Query function

The MondoDB Query Function allows the creation of a function to perform an
MLDB [SQL query](/doc/builtin/sql/Sql.md) against a MongoDB collection. It is
similarl to the
[SQL Query function](/doc/builtin/functions/SqlQueryFunction.md).

## Caveat
- mongodb.dataset will not work on collections having key "_id" that are not
  ObjectIDs. See [MongoDB ObjectID](https://docs.mongodb.com/manual/reference/method/ObjectId/)
- Some less popular BSON types are not supported.
  See [BSON Types](https://docs.mongodb.com/master/reference/bson-types/)
-- Regular Expression
-- DBPointer
-- JavaScript
-- JavaScript (with scope)
-- Min key
-- Max key

## Configuration

![](%%config function mongodb.query)
