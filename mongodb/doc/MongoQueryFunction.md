# MongoDB Query function

The MondoDB Query Function allows the creation of a function to perform an
MLDB [SQL query](/doc/builtin/sql/Sql.md) against a MongoDB collection. It is
similar to the
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

## Example

For this example, we will use a MongoDB database populated with data provided by
the book MongoDB In Action. The zipped json file is available at
[http://mng.bz/dOpd](http://mng.bz/dOpd).

Here we create the query function on the MongoDB zips database zips collection.

```python
mldb.put("/v1/functions/mongo_query", {
    "type": "mongodb.query",
    "params": {
        "connectionScheme": 'mongodb://khan.mldb.ai:11712/zips',
        "collection": 'zips'
    }
})
```

A direct call to the function looks like

```python
import json
mldb.get('/v1/functions/mongo_query/application',
    input={'query' : json.dumps({'zip' : {'$eq' : '60623'}})}
).json()
```

With the output

```json
{
    'output': {
        '_id': u'57d2f5eb21af5ee9c4e22302',
        'city': 'CHICAGO',
        'loc': [['x', [87.7157, '2016-09-09T17:48:27Z']],
                ['y', [41.849015, '2016-09-09T17:48:27Z']]],
        'pop': 112047,
        'state': 'IL',
        'zip': '60623'
    }
}
```

Here is an example of the function beign used within a query.

```python
mldb.query("""
    SELECT mongo_query({query: '{"loc.x" : {"$eq" : 73.968312}}'}) AS *
""")
```

| _id | city | loc.x | loc.y | pop | state | zip |
|-----|------|-------|-------|-----|-------|-----|
| _rowName |
| result | 57d2f5eb21af5ee9c4e24e4f | NEWORK | 73.968312 | 40.797466 | 100027 | NY | 10025
