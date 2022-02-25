# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#
# reddit_benchmark.py
# Mich, 2015-11-27
# Copyright (c) 2015 mldb.ai inc. All rights reserved.
#

import sys
import os
import csv

import unittest
import tempfile

from mldb import mldb, MldbUnitTest, ResponseException

tmp_dir = os.getenv('TMP')
limit = 100000

res = mldb.put('/v1/procedures/load_json_reviews', {
    "type": "import.json",
    "params": {
        'runOnCreation': True,
        'outputDataset':{'id' : 'reviews_json', 'type' : 'tabular'},
        'select': "* EXCLUDING (reviewText), regex_replace(reviewText,'([a-z])\\.([A-Z])','\\1.\n\\2') AS reviewText, lineNumber() AS linenumber",
        "dataFileUrl": "file://mldb_test_data/reviews_Digital_Music_5.json.zstd",
        'limit': limit
    }
})

mldb.log(res.json())
assert res.status_code == 201, str(res)

tmp_file = tempfile.NamedTemporaryFile(dir=tmp_dir, suffix='.csv')

res = mldb.put('/v1/procedures/export_csv_reviews', {
    "type": "export.csv",
    "params": {
        'runOnCreation': True,
        'exportData' : 'SELECT * FROM reviews_json ORDER BY linenumber',
        'dataFileUrl' : 'file://' + tmp_file.name
    }
})

mldb.log(res.json())
assert res.status_code == 201, str(res)

res = mldb.put('/v1/procedures/load_csv_reviews', {
    "type": "import.text",
    "params": {
        'runOnCreation': True,
        'outputDataset':{'id' : 'reviews_csv', 'type' : 'tabular'},
        "dataFileUrl": 'file://' + tmp_file.name,
        'named': 'linenumber',
        'allowMultiLines': True,
    }
})

res = mldb.query('SELECT * FROM reviews_csv JOIN reviews_json ON reviews_csv.rowName() = reviews_json.rowName() WHERE reviews_csv.reviewText != reviews_json.reviewText LIMIT 5')

mldb.log(res)
assert len(res) == 1, str(res)
