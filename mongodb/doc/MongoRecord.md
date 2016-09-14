# MongoDB Record Dataset

The MongoDB Record Dataset is a write-only
[dataset](/doc/builtin/datasets/Datasets.md) that writes to a MongoDB
collection.

Rows are stored in collections with the following format:

```
{
    "_id" : <row name>,
    "<column name>", <column value>,
    ...
}
```

## Caveats
* Timestamps are not recorded into MongoDB.
* Column names containing a dot (.) or starting with a dollar sign ($) will
  fail to be recorded as MongoDB doesn't support them.
* Trying to record a row with a row name that was already recorded will fail.

## Configuration

![](%%config dataset mongodb.record)
