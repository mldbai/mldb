# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import random, json

dataset = mldb.create_dataset({ "type": "sparse.mutable", "id": "x" })

try:
    res = dataset.record_row(0, [])
except:
    assert False, "No exception expected"
else:
    pass

for r  in range(100):
    dataset.record_row( r, [ [c, random.random(), 0] for c in range(100)] )
for r in range(100, 200):
    dataset.record_row( str(r), [ [c, random.random(), 0] for c in range(100)] )
dataset.commit()

res = mldb.perform("GET", "/v1/query", [["q", "select * from x"]],{})
if res["statusCode"] == 200:
    mldb.script.set_return("success")
else:
    mldb.log(json.dumps(res))
