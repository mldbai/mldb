# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json

def check_res(res, code):
    if res['statusCode'] != code:
        mldb.log(json.loads(res['response']))
        assert False

conf = {
    "type": "sql.expression",
    "params": {
        "expression": "input.x*2 as x2, input.y*2 as y2"
    }
}
rez = mldb.perform("PUT", "/v1/functions/f1", [], conf)
check_res(rez, 201)

conf2 = {
    "type": "sql.expression",
    "params": {
        "expression": "input.x3*2 as x4, input.y3*2 as y4"
    }
}
rez = mldb.perform("PUT", "/v1/functions/f2", [], conf2)
check_res(rez, 201)


rez = mldb.perform("GET", "/v1/query", [["q", "select f1( {input: {x: 1, y: 2}} ) as *"]])
check_res(rez, 200)

jsRez = json.loads(rez["response"])
mldb.log(jsRez)

assert jsRez[0]['columns'][0][1] == 2
assert jsRez[0]['columns'][1][1] == 4

rez = mldb.perform("GET", "/v1/query", [["q", """select f2( {input: f1( {input: {x: 1, y: 2}} )[{x3: x2, y3: y2}] }) as * """]])
check_res(rez, 200)

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
check_res(rez, 201)
mldb.log(rez)

rez = mldb.perform("GET", "/v1/query", [["q", "select f3( { {{ 42 as x } as nested} as input } ) as *"]])
check_res(rez, 200)
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

assert jsRez[0]['columns'][0][1] == 42

mldb.script.set_return("success")


check_res(mldb.perform("PUT", "/v1/functions/a", [], {
    "type": "sql.expression",
    "params": { "expression": "abs(input) as output" }
}), 201)

check_res(mldb.perform("PUT", "/v1/functions/b", [], {
    "type": "sql.expression",
    "params": { "expression": "a({input})[output] as output" }
}), 201)

check_res(mldb.perform("PUT", "/v1/functions/c", [], {
    "type": "sql.expression",
    "params": { "expression": "b({input})[output] as output" }
}), 201)

# this segfaults - see MLDB-1250
#rez = mldb.perform("GET", "/v1/query", [["q", "select c({input: -1})"]])
#check_res(rez, 200)


check_res(mldb.perform("PUT", "/v1/functions/recurse", [], {
    "type": "sql.expression",
    "params": { "expression": "recurse({input})[output] as output" }
}), 201)

# this hangs - see MLDB-1251
#rez = mldb.perform("GET", "/v1/query", [["q", "select recurse({input: -1})"]])
#check_res(rez, 500)

