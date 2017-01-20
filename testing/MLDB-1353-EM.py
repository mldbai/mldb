# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

import unittest, json

mldb = mldb_wrapper.wrap(mldb) # noqa

csv_conf = {
    "type": "import.text",
    "params": {
        'dataFileUrl' : 'file://mldb/testing/dataset/iris.data',
        "outputDataset": {
            "id": "iris",
        },
        "runOnCreation": True,
        "headers": [ "sepal length", "sepal width", "petal length", "petal width", "class" ]
    }
}
mldb.put("/v1/procedures/csv_proc", csv_conf) 

mldb.put('/v1/procedures/em_train_iris', {
    'type' : 'gaussianclustering.train',
    'params' : {
        'trainingData' : 'select * EXCLUDING(class) from iris',
        'outputDataset' : 'iris_clusters',
        'numClusters' : 3,
        'modelFileUrl' : "file://tmp/MLDB-1353.gs",
        "runOnCreation": True
    }
})

res = mldb.query("""
    select pivot(class, num) as *
    from (
        select cluster, class, count(*) as num
        from merge(iris_clusters, iris)
        group by cluster, class
    )
    group by cluster
""")

# mldb.log(res)

expected = [
    [
        "_rowName",
        "Iris-versicolor",
        "Iris-setosa",
        "Iris-virginica"
    ],
    [
        "[0]",
        45,
        None,
        None
    ],
    [
        "[1]",
        None,
        50,
        None
    ],
    [
        "[2]",
        5,
        None,
        50
    ]
]

assert res == expected

# test the function

mldb.put('/v1/functions/em_function', {
    'type' : 'gaussianclustering',
    'params' : {       
        'modelFileUrl' : "file://tmp/MLDB-1353.gs",
    }
})

expected = mldb.query("select * from iris_clusters order by rowName()" )
#mldb.log(expected)

result = mldb.query("select em_function({{* EXCLUDING(class)} as embedding}) from iris order by rowName()")
#mldb.log(result)

#check that the function returns the same as the output dataset
for i in range(1, 151):
	assert expected[i] == result[i]

mldb.script.set_return("success")
