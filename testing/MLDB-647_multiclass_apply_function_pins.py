# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


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


result = mldb.perform("PUT", "/v1/procedures/iris_cls", [], {
    'type' : 'classifier.train',
    'params' : {
        'trainingDataset' : {'id' : 'iris_dataset'},
        "configuration": {
            "type": "decision_tree",
            "max_depth": 8,
            "verbosity": 3,
            "update_alg": "prob"
        },
        "modelFileUrl": "file://tmp/MLDB-647.cls",
        'select' : "* EXCLUDING(class)",
        'label' : 'class',
        'where': "rowHash() % 15 = 0",
        "mode": "categorical"
    }
})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("POST", "/v1/procedures/iris_cls/runs", [], {})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("PUT", "/v1/functions/iris_cls_blk", [], {
    'type' : 'classifier',
    'params' : { "modelFileUrl": "file://tmp/MLDB-647.cls" }
})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("GET", "/v1/query", [["q", '''select 
iris_cls_blk({{ * EXCLUDING(class)} as features}) 
from iris_dataset''' ]], {})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("GET", "/v1/datasets/iris_dataset/query", [["select", 
'''iris_cls_blk({{* EXCLUDING(class)} as features})''']], {})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("GET", "/v1/query", [["q", '''select 
iris_cls_blk({{* EXCLUDING(class)} as features})["scores.""Iris-setosa"""] 
from iris_dataset''' ]], {})
assert result["statusCode"] < 400, result["response"]


result = mldb.perform("GET", "/v1/datasets/iris_dataset/query", [["select", 
'''iris_cls_blk({{* EXCLUDING(class)} as features})["scores.""Iris-setosa"""] AS setosa''']], {})
assert result["statusCode"] < 400, result["response"]
mldb.script.set_return("success")


result = mldb.perform("PUT", "/v1/functions/iris_cls_exp", [], {
    'type' : 'classifier.explain',
    'params' : { "modelFileUrl": "file://tmp/MLDB-647.cls" }
})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("GET", "/v1/query", [["q", '''select 
iris_cls_exp({{* EXCLUDING(class)} as features, class as label}) 
from iris_dataset''' ]], {})
assert result["statusCode"] < 400, result["response"]
