# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


# create a mutable beh dataset
datasetConfig = {
    "type": "sparse.mutable",
    "id": "dontCreateTwice"
}

dataset = mldb.create_dataset(datasetConfig)

try:
    dataset = mldb.create_dataset(datasetConfig)
except Exception as e:
    mldb.log(str(e))
    if str(e) == "dataset entry 'dontCreateTwice' already exists":
        mldb.script.set_return("success")
    else:
        raise e

