# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@datacratic.com
# @Date:               2015-04-02 15:20:49
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-14 13:47:59
# @File Name:          MLDB-485-svd_embedRow_returns_zeroes.py


import json
from collections import Counter
import numpy as np
import datetime
import sys
import random


def random_dataset():
    datasetConfig = {
            "type": "sparse.mutable",
            "id": "random_dataset"
        }

    dataset = mldb.create_dataset(datasetConfig)
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
    'params':
    {
        "trainingDataset": {"id": "random_dataset"},
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
        "select": "*",
        "modelFileUrl": "file://tmp/MLDB-485.svd.json.gz"
    }
}

r = mldb.perform("GET", '/v1/datasets/random_dataset/query', [], '{}')
#mldb.log(json.dumps(json.loads(r["response"]), indent=4))

r = mldb.perform("PUT", svd_procedure, [], svd_config)
#mldb.log(json.dumps(json.reads(r.response), indent=4))

mldb.log("Status code for creating the procedure: {}".format(r["statusCode"]))

# Training the procedure
r = mldb.perform("PUT", svd_procedure + "/runs/1", [], {})
if not 300 > r["statusCode"] >= 200:
    mldb.log(json.dumps(r, indent=4))
    mldb.script.set_return("FAILURE")
    mldb.log(str(r["statusCode"]))


# Creating the function
svd_function = "/v1/functions/svd_function"
svdFunctionConfig = {
        "id": "svd_function",
        "type": "svd.embedRow",
        "params": { "modelFileUrl": "file://tmp/MLDB-485.svd.json.gz", "maxSingularValues": 20 }
    }
r = mldb.perform("PUT", svd_function, [], svdFunctionConfig)
if not 300 > r["statusCode"] >= 200:
    mldb.log(json.dumps(r, indent=4))
    mldb.script.set_return("FAILURE")
    mldb.log(str(r["statusCode"]))
    sys.exit(0)


phrase = "1 5 8 1 5 7"
c = Counter(phrase.split(" "))
words = {}
for word, count in c.items():
    words[str(word)] =  count

input = { "row": words }

r = mldb.perform("GET", "/v1/functions/svd_function/application", [["input", input]], {})
mldb.log(r["response"])
mldb.log(json.dumps(json.loads(r["response"]), indent=4))
features = np.array(json.loads(r["response"])["output"]["embedding"])

if not features[features > 0].any():
    mldb.script.set_return("FAILURE")
    mldb.log("Features were {}".format(features))
    mldb.log(str(r["statusCode"]))
    sys.exit(0)

else:
    mldb.script.set_return("success")
