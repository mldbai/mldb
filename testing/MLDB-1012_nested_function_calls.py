# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json

conf = {
    "type": "sql.expression",
    "params": {
        "expression": "input.x*2 as x2, input.y*2 as y2"
    }
}
rez = mldb.perform("PUT", "/v1/functions/f1", [], conf)
mldb.log(rez)

conf2 = {
    "type": "sql.expression",
    "params": {
        "expression": "input.x3*2 as x4, input.y3*2 as y4"
    }
}
rez = mldb.perform("PUT", "/v1/functions/f2", [], conf2)
mldb.log(rez)


rez = mldb.perform("GET", "/v1/query", [["q", "select f1( {input: {x: 1, y: 2}} ) as *"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

assert jsRez[0]['columns'][0][1] == 2
assert jsRez[0]['columns'][1][1] == 4

rez = mldb.perform("GET", "/v1/query", [["q", """select f2( {input: f1( {input: {x: 1, y: 2}} )[{x3: x2, y3: y2}] }) as * """]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

assert jsRez[0]['columns'][0][1] == 4
assert jsRez[0]['columns'][1][1] == 8

#Test for 3-deep nested arguments

conf3 = {
    "type": "sql.expression",
    "params": {
        "expression": "input.nested.x as foo"
    }
}
rez = mldb.perform("PUT", "/v1/functions/f3", [], conf3)
mldb.log(rez)

rez = mldb.perform("GET", "/v1/query", [["q", "select f3( { {{ 42 as x } as nested} as input } ) as *"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

assert jsRez[0]['columns'][0][1] == 42

mldb.script.set_return("success")


