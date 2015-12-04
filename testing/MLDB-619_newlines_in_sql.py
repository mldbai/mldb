# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import random, json

dataset = mldb.create_dataset({ "type": "sparse.mutable", "id": "x" })
dataset.record_row("rowname", [["colname", 0, 0]])
dataset.commit()

res = [
    mldb.perform("GET", "/v1/datasets/x/query", [["where", "colname=\n1"]],{}),
    mldb.perform("GET", "/v1/query", [["q", "select *\nfrom x"]],{})
    ]

broken = False
for r in res:
    if r["statusCode"] != 200:
        broken = True
        mldb.log(json.dumps(r))

if not broken:
    mldb.script.set_return("success")
