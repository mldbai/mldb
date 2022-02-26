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

languages = { 'en' } # { "de", "en", "es", "fr", "ja", "zh" }
dataset = "train"  # "dev"  # "train"
limit = 200000

for language in languages:
    res = mldb.put(f'/v1/procedures/load_json_multilingual_reviews_{language}', {
        "type": "import.json",
        "params": {
            'runOnCreation': True,
            'outputDataset':{'id' : f'reviews_json_{language}', 'type' : 'tabular'},
            'select': "* EXCLUDING(review_body), regex_replace(review_body,'([a-z])\\.([A-Z])','\\1.\n\\2') AS review_body, lineNumber() AS linenumber",
            "dataFileUrl": f"file://mldb_test_data/multilingual_reviews/dataset_{language}_{dataset}.json.zst",
            'limit': limit
        }
    })

    mldb.log(res.json())
    assert res.status_code == 201, str(res)

    # Try both compressed and non-compressed to exercise all code paths
    for suffix in {".csv",".csv.zstd"}:

        tmp_file = tempfile.NamedTemporaryFile(dir=tmp_dir, suffix=suffix)
        tmp_file_name = tmp_file.name

        res = mldb.put(f'/v1/procedures/export_csv_reviews_{language}', {
            "type": "export.csv",
            "params": {
                'runOnCreation': True,
                'exportData' : f'SELECT * FROM reviews_json_{language} ORDER BY linenumber',
                'dataFileUrl' : 'file://' + tmp_file_name
            }
        })

        mldb.log(res.json())
        assert res.status_code == 201, str(res)

        res = mldb.put('/v1/procedures/load_csv_reviews', {
            "type": "import.text",
            "params": {
                'runOnCreation': True,
                'outputDataset':{'id' : f'reviews_csv_{language}', 'type' : 'tabular'},
                "dataFileUrl": 'file://' + tmp_file_name,
                'named': 'linenumber',
                'allowMultiLines': True,
            }
        })

        mldb.log(res.json())
        assert res.status_code == 201, str(res)
        assert res.json()["status"]["firstRun"]["status"]["rowCount"] == limit
        assert res.json()["status"]["firstRun"]["status"]["numLineErrors"] == 0

        res = mldb.query(f'SELECT reviews_csv.review_body, reviews_json.review_body FROM reviews_csv_{language} AS reviews_csv OUTER JOIN reviews_json_{language} AS reviews_json ON reviews_csv.rowName() = reviews_json.rowName() WHERE CAST (reviews_csv.reviewText AS STRING) != reviews_json.reviewText LIMIT 5')

        mldb.log(res)
        assert len(res) == 1, str(res)

datafile = "file://mldb_test_data/reviews_Digital_Music_5.json.zstd"
#datafile = "file://mldb_test_data/Books_5.json.zstd"
limit = 64706

res = mldb.put('/v1/procedures/load_json_reviews', {
    "type": "import.json",
    "params": {
        'runOnCreation': True,
        'outputDataset':{'id' : 'reviews_json', 'type' : 'tabular'},
        'select': "* EXCLUDING (reviewText, image), regex_replace(reviewText,'([a-z])\\.([A-Z])','\\1.\n\\2') AS reviewText, lineNumber() AS linenumber",
        "dataFileUrl": datafile,
        'limit': limit
    }
})

mldb.log(res.json())
assert res.status_code == 201, str(res)

# Try both compressed and non-compressed to exercise all code paths
for suffix in {".csv",".csv.zstd"}:

    tmp_file = tempfile.NamedTemporaryFile(dir=tmp_dir, suffix=suffix)
    tmp_file_name = tmp_file.name

    res = mldb.put('/v1/procedures/export_csv_reviews', {
        "type": "export.csv",
        "params": {
            'runOnCreation': True,
            'exportData' : 'SELECT * FROM reviews_json ORDER BY linenumber',
            'dataFileUrl' : 'file://' + tmp_file_name
        }
    })

    mldb.log(res.json())
    assert res.status_code == 201, str(res)

    res = mldb.put('/v1/procedures/load_csv_reviews', {
        "type": "import.text",
        "params": {
            'runOnCreation': True,
            'outputDataset':{'id' : 'reviews_csv', 'type' : 'tabular'},
            "dataFileUrl": 'file://' + tmp_file_name,
            'named': 'linenumber',
            'allowMultiLines': True,
        }
    })

    mldb.log(res.json())
    assert res.status_code == 201, str(res)
    assert res.json()["status"]["firstRun"]["status"]["rowCount"] == limit
    assert res.json()["status"]["firstRun"]["status"]["numLineErrors"] == 0

    res = mldb.query('SELECT reviews_csv.reviewText, reviews_json.reviewText FROM reviews_csv OUTER JOIN reviews_json ON reviews_csv.rowName() = reviews_json.rowName() WHERE CAST (reviews_csv.reviewText AS STRING) != reviews_json.reviewText LIMIT 5')

    mldb.log(res)
    assert len(res) == 1, str(res)

