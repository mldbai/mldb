#
# GLDB-800-nested_sql_query.py
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

try:
    mldb.get('/v1/functions/poil2/info')
except mldb_wrapper.ResponseException as exc:
    res = exc.response
else:
    assert False, 'should not be here'
# Uncomment this for the MLDB-801 test-case
#assert res['statusCode'] == 200
# also, check that the output looks like this:
# {
#   "input" : {
#       "pins" : {
#          "x" : {
#             "valueInfo" : {
#                "type" : "Datacratic::MLDB::AnyValueInfo"
#             }
#          }
#       }
#    },
#    "output" : {
#       "pins" : {
#          "x" : {
#             "valueInfo" : {
#                "kind" : "scalar",
#                "scalar" : "Datacratic::MLDB::CellValue",
#                "type" : "Datacratic::MLDB::AtomValueInfo"
#             }
#          },
#          "y" : {
#             "valueInfo" : {
#                "kind" : "scalar",
#                "scalar" : "Datacratic::MLDB::CellValue",
#                "type" : "Datacratic::MLDB::AtomValueInfo"
#             }
#          }
#       }
#    }
# }

mldb.log("poil2 function info")
mldb.log(res.json())

res = mldb.get('/v1/datasets/ds1/query', select='poil2({*})')
mldb.log("query result")
mldb.log(res)


#MLDBFB-480

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

# uncomment for failing case
#assert expected == mldb.query("select patate2({param: 'hi'}) as *")


#MLDB-1573

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
        ["a.1", 1, 0],
        ["a.2", 2, 0]
    ]
})

mldb.post('/v1/datasets/exampleA/rows', {
    "rowName": "second row",
    "columns": [
        ["a.1", 3, 0],
        ["a.2", 4, 0]
    ]
})
mldb.post('/v1/datasets/exampleA/rows', {
    "rowName": "third row",
    "columns": [
        ["a.1", 5, 0],
        ["a.2", 6, 0]
    ]
})
mldb.post('/v1/datasets/exampleB/rows', {
    "rowName": "first row",
    "columns": [
        ["b.1", 10, 0],
        ["b.2", 20, 0]
    ]
})

mldb.post("/v1/datasets/exampleA/commit")
mldb.post("/v1/datasets/exampleB/commit")

mldb.put("/v1/functions/patate", {
    "type": "sql.query",
    "params": {
        "query": """
            SELECT vertical_avg(norm(vector_diff({exampleA.a.*}, {exampleB.b.*}), 2)) as score
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

assert expected == res

mldb.script.set_return('success')
