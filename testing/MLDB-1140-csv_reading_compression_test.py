#
# MLDB-1140-csv_reading_compression_test.py
# Mich, 2015-11-23
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

for ext in ['lz4', 'zip']:
    csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'file://mldb/testing/MLDB-1140-small_score.csv.' + ext,
                "outputDataset": {
                    "id": "score_small",
                },
                "runOnCreation": True,
                "headers" : ["uid", "timestamp", "score"],
                "delimiter" : "\t",
                "offset" : 1,
                "named" : "uid"
            }
        }
    mldb.put("/v1/procedures/csv_proc", csv_conf)   

mldb.script.set_return("success")
