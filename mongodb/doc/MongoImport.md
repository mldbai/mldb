# MongoDB Import procedure

The MongoDB Import Procedure type is used to import a MongoDB collection into a
dataset.

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

![](%%config procedure mongodb.import)

## Example

For this example, we will use a MongoDB database populated with data provided by
the book MongoDB In Action. The zipped json file is available at
[http://mng.bz/dOpd](http://mng.bz/dOpd).

Here we import the zips collection into an MLDB dataset called `mongodb_zips`.

```python
mldb.post('/v1/procedures', {
    'type' : 'mongodb.import',
    'params' : {
        'connectionScheme': 'mongodb://somehost.mldb.ai:11712/zips',
        'collection': 'zips',
        'outputDataset' : {
            'id' : 'mongodb_zips',
            'type' : 'sparse.mutable'
        }
    }
})
```

We can now query the imported data as we would any other MLDB Dataset.

```python
mldb.query("SELECT * FROM mongodb_zips LIMIT 5")
```
| _id | city | loc.x | loc.y | pop | state | zip |
|-----|------|-------|-------|-----|-------|-----|
| _rowName |
| 57d2f5eb21af5ee9c4e27f08 | 57d2f5eb21af5ee9c4e27f08 | BONDURANT | 110.335287 | 43.223798 | 116 | WY | 82922 |
| 57d2f5eb21af5ee9c4e27f07 | 57d2f5eb21af5ee9c4e27f07 | KAYCEE | 106.563230 | 43.723625 | 876 | WY | 82639 |
| 57d2f5eb21af5ee9c4e27f05 | 57d2f5eb21af5ee9c4e27f05 | CLEARMONT | 106.458071 | 44.661010 | 350 | WY | 82835 |
| 57d2f5eb21af5ee9c4e27f03 | 57d2f5eb21af5ee9c4e27f03 | ARVADA | 106.109191 | 44.689876 | 107 | WY | 82831 |
| 57d2f5eb21af5ee9c4e27f01 | 57d2f5eb21af5ee9c4e27f01 | COKEVILLE | 110.916419 | 42.057983 | 905 | WY | 83114 |

Here we did not provide any named parameter so oid() was used. This is why
`_rowName` and `_id` have the same values.

Another element to note is how the loc object was imported. The sub object was
disassembled and imported as loc.x and loc.y into MLDB.
