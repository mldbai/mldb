# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@datacratic.com
# @Date:               2015-04-02 15:20:49
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-02 15:30:05
# @File Name:          MLDB-452-svd_procedure.py

import requests
import datetime
import json

datasetConfig = {
        "type": "beh",
        "id": "tweets_dataset",
        "address": "/home/atremblay/workspace/platform/tweets_dataset.beh.gz"
    }

dataset = mldb.create_dataset(datasetConfig)

svd_procedure = "/v1/procedures/svd_tweets"

svd_config = {
    'type' : 'svd.train',
    'params' :
    {
        "trainingData": {"from" : {"id": "tweets_dataset"},
                         "select": "*",
                         "where": "rowHash() % 10 = 0"
                     },
        "columnOutputDataset": {
            "type": "embedding",
            "id": "svd_tweets_col"
        },
        "rowOutputDataset": {
            "id": "svd_tweets_row",
            'type': "embedding",
            'address' : "svd_tweets_row.beh.gz"
        },
        "numSingularValues": 200,
        "numDenseBasisVectors": 500
    }
}

r = mldb.perform("PUT", svd_procedure, [], svd_config)
print r
print("Status code for creating the procedure: {}".format(r["statusCode"]))

r = mldb.perform("PUT", svd_procedure + "/runs/1", [], {})
if not 300 > r["statusCode"] >= 200:
    print r
    mldb.script.set_return("FAILURE")
    print(r["statusCode"])

else:
    mldb.script.set_return("success")
