# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#
# TODO
# Mich, 2015-11-23
# Copyright (c) 2015 Datacratic Inc. All rights reserved.
#

if False:
    mldb = None

for ext in ['lz4', 'gz', 'zip']:
    mldb.log("Testing extension: " + ext)
    res = mldb.perform('PUT', '/v1/datasets/score_small_' + ext, [], {
        "type": "text.csv.tabular",
        "params": {
            "dataFileUrl": "file://mldb/testing/MLDB-1140-small_score.csv." + ext,
            "headers" : ["uid", "timestamp", "score"],
            "delimiter" : "\t",
            "offset" : 1,
            "named" : "uid"
        }
    })
    assert res['statusCode'] == 201, str(res)

mldb.script.set_return("success")
