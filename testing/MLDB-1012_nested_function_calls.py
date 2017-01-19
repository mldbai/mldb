#
# MLDB-1012_nested_function_calls.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa

conf = {
    "type": "sql.expression",
    "params": {
        "expression": "input.x*2 as x2, input.y*2 as y2"
    }
}
mldb.put("/v1/functions/f1", conf)

conf2 = {
    "type": "sql.expression",
    "params": {
        "expression": "input.x3*2 as x4, input.y3*2 as y4"
    }
}
mldb.put("/v1/functions/f2", conf2)

rez = mldb.get("/v1/query", q="select f1( {input: {x: 1, y: 2}} ) as *")
js_rez = rez.json()
mldb.log(js_rez)

assert js_rez[0]['columns'][0][1] == 2
assert js_rez[0]['columns'][1][1] == 4

rez = mldb.get("/v1/query",
               q="""select f2( {input: f1( {input: {x: 1, y: 2}} )
                    [{x3: x2, y3: y2}] }) as * """)

js_rez = rez.json()
mldb.log(js_rez)

assert js_rez[0]['columns'][0][1] == 4
assert js_rez[0]['columns'][1][1] == 8

# Test for 3-deep nested arguments

conf3 = {
    "type": "sql.expression",
    "params": {
        "expression": "input.nested.x as foo"
    }
}
rez = mldb.put("/v1/functions/f3", conf3)
mldb.log(rez)

rez = mldb.get("/v1/query",
               q="select f3( { {{ 42 as x } as nested} as input } ) as *")
js_rez = rez.json()
mldb.log(js_rez)

assert js_rez[0]['columns'][0][1] == 42


mldb.put("/v1/functions/a", {
    "type": "sql.expression",
    "params": {"expression": "abs(input) as output"}
})

mldb.put("/v1/functions/b", {
    "type": "sql.expression",
    "params": {"expression": "a({input})[output] as output"}
})

mldb.put("/v1/functions/c", {
    "type": "sql.expression",
    "params": {"expression": "b({input})[output] as output"}
})

rez = mldb.get("/v1/query", q="select c({input: -1})")
js_rez = rez.json()
assert js_rez[0]['columns'][0][1] == 1

# MLDB-1251
mldb.log("MLDB-1251")
try:
    mldb.put("/v1/functions/recurse", {
        "type": "sql.expression",
        "params": {"expression": "recurse({input})[output] as output"}
    })

    mldb.get("/v1/query", q="select recurse({input: -1})")
except mldb_wrapper.ResponseException as exc:
    pass
else:
    assert False, 'Should have failed with a 400'

mldb.script.set_return("success")
