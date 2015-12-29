# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json
from urllib import urlopen


dataset = mldb.create_dataset({ "type": "sparse.mutable", "id": "iris_dataset" })

with open("./mldb/testing/dataset/iris.data") as f:
    for i, line in enumerate(f):
        
        cols = []
        line_split = line.split(',')
        if len(line_split) != 5:
            continue
        cols.append(["sepal length", float(line_split[0]), 0])
        cols.append(["sepal width", float(line_split[1]), 0])
        cols.append(["petal length", float(line_split[2]), 0])
        cols.append(["petal width", float(line_split[3]), 0])
        cols.append(["class", line_split[4], 0])
        dataset.record_row(str(i+1), cols)

dataset.commit()

result = mldb.perform("PUT", "/v1/procedures/em", [], {
    'type' : 'EM.train',
    'params' : {
        'dataset' : {'id': 'iris_dataset' },
        'output' : {'id' : 'em_output', 'type' : 'embedding'},
        'centroids' : {'id' : 'em_centroids', 'type' : 'embedding'},            
        'numClusters' : 3,
        'select': '* excluding(class)'
    }
})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("POST", "/v1/procedures/em/runs", [], {})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("GET", "/v1/datasets/em_output/query", [
    ["select", "*" ], ["format", "table"], 
    ["rowNames", "true"]
], {})
assert result["statusCode"] < 400, result["response"]

mldb.script.set_return("success")