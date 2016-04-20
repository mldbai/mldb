#
# MLDB-800-nested_sql_query.py
# datacratic, 2015
# this file is part of mldb. copyright 2015 datacratic. all rights reserved.
#
if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'ds1'})

for i in xrange(5):
    ds1.record_row('row_' + str(i),
                   [['x', 'row_0', 0], ['y', i*2, 0]])
ds1.commit()

res = mldb.put('/v1/functions/patate', {
    'type': 'sql.query',
    'params': {
        'query': 'select * from ds1 where rowName() = $x'
    }
})

res = mldb.get('/v1/functions/patate/info')
mldb.log("patate function info")
mldb.log(res.json())

# WITH (x)
res = mldb.put('/v1/functions/poil', {
        'type': 'sql.expression',
        'params': {
            'expression': 'patate({x})'}})

res = mldb.get('/v1/functions/poil/info')
mldb.log("poil function info")
mldb.log(res.json())

res = mldb.get('/v1/datasets/ds1/query', select='poil({*})')
mldb.log("ds1 query")
mldb.log(res.json())

# same but with WITH (*)
res = mldb.put('/v1/functions/poil2', {
    'type': 'sql.expression',
    'params': {
        'expression': 'patate({*})'}})

res = mldb.get('/v1/functions/poil2/info')
mldb.log(res.json())
#assert res.statusCode == 200

expected = {
    "input": {
        "hasUnknownColumns": True, 
        "type": "Datacratic::MLDB::RowValueInfo", 
        "kind": "row", 
        "knownColumns": []
    }, 
    "output": {
        "hasUnknownColumns": False, 
        "type": "Datacratic::MLDB::RowValueInfo", 
        "kind": "row", 
        "knownColumns": [
            {
                "columnName": "patate({*})", 
                "valueInfo": {
                    "hasUnknownColumns": False, 
                    "type": "Datacratic::MLDB::RowValueInfo", 
                    "kind": "row", 
                    "knownColumns": [
                        {
                            "columnName": "x", 
                            "valueInfo": {
                                "type": "Datacratic::MLDB::AnyValueInfo"
                            }, 
                            "sparsity": "sparse"
                        }, 
                        {
                            "columnName": "y", 
                            "valueInfo": {
                                "type": "Datacratic::MLDB::AnyValueInfo"
                            }, 
                            "sparsity": "sparse"
                        }
                    ]
                }, 
                "sparsity": "dense"
            }
        ]
    }
}

assert res.json() == expected

mldb.log("poil2 function info")
mldb.log(res.json())

res = mldb.get('/v1/datasets/ds1/query', select='poil2({*})')
mldb.log("query result")
mldb.log(res)

mldb.script.set_return('success')
