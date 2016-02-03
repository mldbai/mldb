#
# MLDB-1140-csv_reading_compression_test.py
# Mich, 2015-11-23
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

for ext in ['lz4', 'zip']:
    res = mldb.put('/v1/datasets/score_small_' + ext, {
        "type": "text.csv.tabular",
        "params": {
            "dataFileUrl":
                "file://mldb/testing/MLDB-1140-small_score.csv." + ext,
            "headers" : ["uid", "timestamp", "score"],
            "delimiter" : "\t",
            "offset" : 1,
            "named" : "uid"
        }
    })

mldb.script.set_return("success")
