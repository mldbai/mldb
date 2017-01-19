#
# MLDB-647_multiclass_apply_function_pins.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

dataset = mldb.create_dataset({
    "type": "sparse.mutable",
    "id": "iris_dataset"
})

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


mldb.put("/v1/procedures/iris_cls", {
    'type' : 'classifier.train',
    'params' : {
        'trainingData' : {
            'select' : "{* EXCLUDING(class)} as features, class as label",
            'where': "rowHash() % 15 = 0",
            'from' : {'id' : 'iris_dataset'}
        },
        "configuration": {
            "type": "decision_tree",
            "max_depth": 8,
            "verbosity": 3,
            "update_alg": "prob"
        },
        "modelFileUrl": "file://tmp/MLDB-647.cls",
        "mode": "categorical"
    }
})

mldb.post("/v1/procedures/iris_cls/runs")

mldb.put("/v1/functions/iris_cls_blk", {
    'type' : 'classifier',
    'params' : { "modelFileUrl": "file://tmp/MLDB-647.cls" }
})

mldb.get("/v1/query", q='''select
iris_cls_blk({{ * EXCLUDING(class)} as features})
from iris_dataset''')

mldb.get("/v1/query", q=
'''SELECT iris_cls_blk({{* EXCLUDING(class)} as features}) from iris_dataset''')

mldb.get("/v1/query", q='''select
iris_cls_blk({{* EXCLUDING(class)} as features})[scores."Iris-setosa"]
from iris_dataset''')

mldb.get("/v1/query", q=
'''SELECT iris_cls_blk({{* EXCLUDING(class)} as features})[scores."Iris-setosa"]
   AS setosa FROM iris_dataset''')

mldb.put("/v1/functions/iris_cls_exp", {
    'type' : 'classifier.explain',
    'params' : {"modelFileUrl": "file://tmp/MLDB-647.cls"}
})

mldb.get("/v1/query", q='''select
iris_cls_exp({{* EXCLUDING(class)} as features, class as label})
from iris_dataset''')
mldb.script.set_return("success")
