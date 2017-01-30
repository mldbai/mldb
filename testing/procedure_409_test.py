# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


# Load data
import datetime
import json

datasetConfig = {
        "type": "sparse.mutable",
        "id": "iris_dataset"
    }

dataset = mldb.create_dataset(datasetConfig)
ts = datetime.datetime.now().isoformat(' ')

with open('./mldb/testing/dataset/iris.data') as f:
    for i, line in enumerate(f):
        cols = []
        line_split = line.split(',')
        if len(line_split) != 5:
            continue
        cols.append(["sepal length", float(line_split[0]), ts])
        cols.append(["sepal width", float(line_split[1]), ts])
        cols.append(["petal length", float(line_split[2]), ts])
        cols.append(["petal width", float(line_split[3]), ts])
        cols.append(["class", line_split[4], ts])
        dataset.record_row(str(i+1), cols)

dataset.commit()

svd_procedure = "/v1/procedures/svd_iris"

svd_config = {
    'type' : 'svd.train',
    'params' :
    {
        "trainingData": {"from" : {"id": "iris_dataset"},
                         "select": '"petal width", "petal length", "sepal length", "sepal width"'
                     },
        "columnOutputDataset": {
            "type": "sparse.mutable",
            "id": "svd_iris_col"
        },
        "rowOutputDataset": {
            "id": "iris_svd_row",
            'type': "embedding"
        },
        "numSingularValues": 4,
        "numDenseBasisVectors": 2
    }
}

r = mldb.perform("PUT", svd_procedure, [], svd_config)
print r
print(r["statusCode"])

r = mldb.perform("PUT", svd_procedure + "/runs/1", [], {})
if not 300 > r["statusCode"] >= 200:
    print r
    mldb.script.set_return("FAILURE")
    print(r["statusCode"])

else:
    mldb.script.set_return("success")
