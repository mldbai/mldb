#
# MLDB-800-nested_sql_query.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
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

res = mldb.get('/v1/query', q='SELECT poil({*}) from ds1')
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
    "input": [ {
        "hasUnknownColumns": True, 
        "hasUnknownColumnsRecursive": True, 
        "type": "MLDB::RowValueInfo", 
        "kind": "row", 
        "isConstant": False,
        "knownColumns": []
    } ], 
    "output": {
        "hasUnknownColumns": False, 
        "hasUnknownColumnsRecursive": True, 
        "type": "MLDB::RowValueInfo", 
        "kind": "row", 
        "isConstant": False,
        "knownColumns": [
            {
                "columnName": "patate({*})", 
                "valueInfo": {
                    "hasUnknownColumns": True, 
                    "hasUnknownColumnsRecursive": True, 
                    "type": "MLDB::RowValueInfo", 
                    "kind": "row", 
                    "isConstant": False,
                    "knownColumns": [
                        {
                            "columnName": "x", 
                            "valueInfo": {
                                "type": "MLDB::AnyValueInfo",
                                "isConstant": False
                            }, 
                            "sparsity": "sparse",
                        }, 
                        {
                            "columnName": "y", 
                            "valueInfo": {
                                "type": "MLDB::AnyValueInfo",
                                "isConstant": False
                            }, 
                            "sparsity": "sparse",
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

res = mldb.get('/v1/query', q='SELECT poil2({*}) from ds1')
mldb.log("query result")
mldb.log(res)


#MLDBFB-480

mldb.log("MLDBFB-480")

expected = [["_rowName", "param"],
            ["result", "hi" ]]

res = mldb.put("/v1/functions/patate1", {
    "type": "sql.query",
    "params": {
        "query": """
            select $param as param from ds1
        """,
        "output": "FIRST_ROW"
    }
})

assert expected == mldb.query("select patate1({param: 'hi'}) as *")

res = mldb.put("/v1/functions/patate2", {
    "type": "sql.query",
    "params": {
        "query": """
            select * from ( select $param as param from ds1 )
        """,
        "output": "FIRST_ROW"
    }
})

res = mldb.query("select patate2({param: 'hi'}) as *")

assert expected == res

#MLDB-1573

mldb.log("MLDB-1573")

res = mldb.put("/v1/functions/patate", {
    "type": "sql.query",
    "params": {
        "query": """
            select * from (
                select * from 
                row_dataset({x: 1, y:2, z: 'three'})
            )
        """,
        "output": "FIRST_ROW"
    }
})

res = mldb.query("select patate()")

expected = [["_rowName", "patate().column", "patate().value"],
            ["result", "x", 1 ]]

assert res == expected

#MLDB-1574

mldb.log("MLDB-1574")

mldb.put("/v1/functions/patate", {
    "type": "sql.query",
    "params": {
        "query": """
                select avg(value) from (select * from
                row_dataset({x: 1, y:2, z: 3}))

        """,
        "output": "FIRST_ROW"
    }
})

res = mldb.query("select patate()")

mldb.log(res)

expected = [[ "_rowName", "patate().avg(value)" ],
           [ "result", 2 ]]

assert res == expected

mldb.put('/v1/datasets/exampleA', { "type":"sparse.mutable" })
mldb.put('/v1/datasets/exampleB', { "type":"sparse.mutable" })
mldb.post('/v1/datasets/exampleA/rows', {
    "rowName": "first row",
    "columns": [
        ["a1", 1, 0],
        ["a2", 2, 0]
    ]
})

mldb.post('/v1/datasets/exampleA/rows', {
    "rowName": "second row",
    "columns": [
        ["a1", 3, 0],
        ["a2", 4, 0]
    ]
})
mldb.post('/v1/datasets/exampleA/rows', {
    "rowName": "third row",
    "columns": [
        ["a1", 5, 0],
        ["a2", 6, 0]
    ]
})
mldb.post('/v1/datasets/exampleB/rows', {
    "rowName": "first row",
    "columns": [
        ["b1", 10, 0],
        ["b2", 20, 0]
    ]
})

mldb.post("/v1/datasets/exampleA/commit")
mldb.post("/v1/datasets/exampleB/commit")

mldb.put("/v1/functions/patate", {
    "type": "sql.query",
    "params": {
        "query": """
            SELECT vertical_avg(norm(vector_diff([exampleA.a1, exampleA.a2], [exampleB.b1, exampleB.b2]), 2)) as score
            FROM exampleA JOIN exampleB
        """,
        "output": "FIRST_ROW"
    }
})

res = mldb.query("select patate()")

expected = [
    [
        "_rowName",
        "patate().score"
    ],
    [
        "result",
        17.484976580463197
    ]
]

mldb.log(res)

assert expected == res


res = mldb.put('/v1/functions/fwin', {
    'type': 'sql.query',
    'params': {
        'query': 'select $varrr as hoho from ds1 limit 1'
    }
})


res = mldb.put('/v1/functions/pwel', {
    'type': 'sql.query',
    'params': {
        'query': 'select fwin({varrr: $y}) from ds1 where rowName() = $x'
    }
})


mldb.log("ds1 query")
# This test case fails on binding, so simply not throwing means it's fixed
res = mldb.get('/v1/query', q="select pwel({x:'row_2', y:'prout'}) from ds1")
mldb.log(res.json())


mldb.script.set_return('success')
