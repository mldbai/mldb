# MongoDB Dataset

The MongoDB Dataset is a read only [dataset](/doc/builtin/datasets/Datasets.md)
based on a MongoDB collection. It is meant to be used as a bridge between MLDB
and MongoDB by allowing MLDB SQL queries to run over a MongoDB collection.

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

![](%%config dataset mongodb.dataset)
