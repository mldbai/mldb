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

## Example

For this example, we will use a MongoDB database populated with data provided by
the book MongoDB In Action. The zipped json file is available at
[http://mng.bz/dOpd](http://mng.bz/dOpd).

Here we create a dataset named `mongodb_zips_bridge`.

```python
mldb.put('/v1/datasets/mongodb_zips_bridge', {
    'type' : 'mongodb.dataset',
    'params' : {
        'connectionScheme': 'mongodb://somehost.mldb.ai:11712/zips',
        'collection': 'zips'
    }
})
```

We can directly query it.

```python
mldb.query("SELECT * NAMED zip FROM mongodb_zips_bridge ORDER BY pop DESC LIMIT 5")
```

| _id | city | loc.x | loc.y | pop | state | zip |
|-----|------|-------|-------|-----|-------|-----|
| _rowName |
| 60623 | 57d2f5eb21af5ee9c4e22302 | CHICAGO | 87.715700 | 41.849015 | 112047 | IL | 60623 |
| 11226 | 57d2f5eb21af5ee9c4e24f28 | BROOKLYN | 73.956985 | 40.646694 | 111396 | NY | 11226 |
| 10021 | 57d2f5eb21af5ee9c4e24e7f | NEW YORK | 73.958805 | 40.768476 | 106564 | NY | 10021 |
| 10025 | 57d2f5eb21af5ee9c4e24e4f | NEW YORK | 73.968312 | 40.797466 | 100027 | NY | 10025 |
| 90201 | 57d2f5eb21af5ee9c4e21258 | BELL GARDENS | 118.172050 | 33.969177 | 99568 | CA | 90201 |
