# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

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

mldb.put('/v1/procedures/hdbscan_train_iris', {
    'type' : 'hdbscan_clustering.train',
    'params' : {
        'trainingData' : 'select * EXCLUDING(class) from iris',
        'outputDataset' : 'iris_clusters',
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