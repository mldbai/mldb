# MongoDB Dataset

The MongoDB Dataset is a read only [dataset](/doc/builtin/datasets/Datasets.md)
based on a MongoDB collection. It is meant to be used as a bridge between MLDB
and MongoDB by allowing MLDB SQL queries to run over a MongoDB collection.

## Caveat
- mongodb.dataset will not work on collections having key "_id" that are not
  ObjectIDs.

## Configuration

![](%%config dataset mongodb.dataset)
