# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@mldb.ai
# @Date:               2015-04-02 15:20:49
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-14 13:47:59
# @File Name:          MLDB-485-svd_embedRow_returns_zeroes.py


from collections import Counter
import datetime
import sys
import random

mldb = mldb_wrapper.wrap(mldb) # noqa

def random_dataset():
    dataset_config = {
            "type": "sparse.mutable",
            "id": "random_dataset"
        }

    dataset = mldb.create_dataset(dataset_config)
    ts = datetime.datetime.now().isoformat(' ')

    for i in range(10):
        dataset.record_row(
            str(i+1),
            [[str(j), random.randint(0, 10), ts] for j in range(1,27)])

    dataset.commit()

random_dataset()


# Creating the svd procedure
svd_procedure = "/v1/procedures/svd_random"
svd_config = {
    'type': 'svd.train',
    'params': {
        "trainingData": {"from" : {"id": "random_dataset"}},
        "columnOutputDataset": {
            "type": "embedding",
            "id": "svd_random_col"
        },
        "rowOutputDataset": {
            "id": "svd_random_row",
            'type': "embedding",
        },
        "numSingularValues": 1000,
        "numDenseBasisVectors": 20,
        "modelFileUrl": "file://tmp/MLDB-485.svd.json.gz"
    }
}

r = mldb.put(svd_procedure, svd_config)
#mldb.log(json.dumps(json.reads(r.response), indent=4))

mldb.log("Status code for creating the procedure: {}".format(r.status_code))

# Training the procedure
r = mldb.put(svd_procedure + "/runs/1")

# Creating the function
svd_function = "/v1/functions/svd_function"
svdFunctionConfig = {
    "id": "svd_function",
    "type": "svd.embedRow",
    "params": { "modelFileUrl": "file://tmp/MLDB-485.svd.json.gz", "maxSingularValues": 20 }
}
r = mldb.put(svd_function, svdFunctionConfig)

phrase = "1 5 8 1 5 7"
c = Counter(phrase.split(" "))
words = {}
for word, count in c.items():
    words[str(word)] =  count

input = {"row": words}

r = mldb.get("/v1/functions/svd_function/application", input=input)
mldb.log(r.json())
features = r.json()["output"]["embedding"]

if not len([i for i in features if i > 0]):
    mldb.script.set_return("FAILURE")
    mldb.log("Features were {}".format(features))
    mldb.log(str(r["statusCode"]))
    sys.exit(0)

mldb.script.set_return("success")
