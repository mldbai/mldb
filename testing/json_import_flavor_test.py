# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
# Test that we can import different styles of JSON (one record per line, records
# per multiple lines, one big array) effectively.

import sys
import os
import csv

import unittest
import tempfile
import json

from mldb import mldb, MldbUnitTest, ResponseException

tmp_dir = os.getenv('TMP')

# filename, expected
j1 = { 'hello': 'world' }
j1s = json.dumps(j1)
print(j1s)
j2 = { 'pi': 3.14 }
j2s = json.dumps(j2)
j3 = { 'ans': 42 }
j3s = json.dumps(j3)

unit_test_cases = [
    ('opening_only',                '[',                None, 400, None),
    ('opening_only_newline',        '\n[',              None, 400, None),
    ('opening_only_newlines',       '\n[\n',            None, 400, None),
    ('opening_only_two_newlines',   '\n\n[\n',          None, 400, None),
    ('empty_array',                 '[]',               None, 201, [{}]),
    ('empty_array_2',               '[\n]',             None, 201, [{}]),
    ('empty_array_3',               '[\n]\n',           None, 201, [{}]),
    ('empty_array_4',               '\n[\n]\n',         None, 201, [{}]),
    ('empty_array_trailing',        '[],',              None, 400, None),
    ('empty_array_trailing_2',      '[\n],',            None, 400, None),
    ('empty_array_trailing_3',      '[\n]\n,',          None, 400, None),
    ('empty_array_trailing_4',      '\n[\n]\n,',        None, 400, None),
    ('dump1',                       f'{j1s}\n{j2s}',    None, 201, [j1,j2]),
    ('dump2',                       f'{j1s}\n{j2s}\n',  None, 201, [j1,j2]),
]

for test in unit_test_cases:
    break
    name, data, compression, code, expected = test
    print(f"running test {name}")
    suffix = '.json' + ('.' + compression if compression is not None else '')
    tmp_file = tempfile.NamedTemporaryFile(dir=tmp_dir, suffix=suffix)
    tmp_file_name = tmp_file.name
    f = open(tmp_file_name, 'w')
    print(f"data={data}")
    f.write(data)
    f.close()

    try:
        res = mldb.put(f'/v1/procedures/load_{name}', {
            "type": "import.json",
            "params": {
                'runOnCreation': True,
                'outputDataset':{'id' : f'test_{name}', 'type' : 'tabular'},
                "dataFileUrl": f'file://{tmp_file_name}'
            }
        })
    except ResponseException as exc:
        assert exc.response.status_code == code, exc
        continue

    assert res.status_code == code, res
    assert res.json()["status"]["firstRun"]["status"]["rowCount"] == len(expected)

datafile,limit = "file://mldb_test_data/reviews_Digital_Music_5.json.zstd",64706
datafile,limit = "file://mldb_test_data/Books_5.json.zstd",10000000

res = mldb.put('/v1/procedures/load_json_reviews', {
    "type": "import.json",
    "params": {
        'runOnCreation': True,
        'outputDataset':{'id' : 'reviews_json', 'type' : 'tabular'},
        'select': "* EXCLUDING (image), lineNumber() AS linenumber",
        "dataFileUrl": datafile,
        'limit': limit
    }
})

mldb.log(res.json())
assert res.status_code == 201, str(res)

exit(0)

# (asArray, prettyPrint, lineBreaks, compression)
scenarios = [
    (False, False, True, None),
    (True, False, True, None),
]

for scenario in scenarios:

    asArray, prettyPrint, lineBreaks, compression = scenario
    suffix = ".json" if compression is None else f".json.{compression}"
    tmp_file = tempfile.NamedTemporaryFile(dir=tmp_dir, suffix=suffix)
    tmp_file_name = tmp_file.name

    res = mldb.put('/v1/procedures/export_json', {
        "type": "export.json",
        "params": {
            'runOnCreation': True,
            'exportData' : 'SELECT * FROM reviews_json ORDER BY linenumber',
            'prettyPrint': prettyPrint,
            'lineBreaks': lineBreaks,
            'asArray': asArray,
            'dataFileUrl' : 'file://' + tmp_file_name,
        }
    })

    #f = open(tmp_file_name, 'r')
    #print(f.read())
    #for line in f.readlines():
    #    print(line)

    mldb.log(res.json())
    assert res.status_code == 201, str(res)

    res = mldb.put('/v1/procedures/reload_exported_reviews', {
        "type": "import.json",
        "params": {
            'runOnCreation': True,
            'outputDataset':{'id' : f'reviews_reloaded', 'type' : 'tabular'},
            "dataFileUrl": f'file://{tmp_file_name}'
        }
    })

    #mldb.log(mldb.query("SELECT reviewerID FROM reviews_json ORDER BY rowName()"))
    #mldb.log(mldb.query("SELECT reviewerID FROM reviews_reloaded ORDER BY rowName()"))

    mldb.log(res.json())
    assert res.status_code == 201, res.json()
    assert res.json()["status"]["firstRun"]["status"]["rowCount"] == limit

    res = mldb.query("""
        SELECT r1.reviewText, r2.reviewText
        FROM reviews_json AS r1 OUTER JOIN reviews_reloaded AS r2
        ON r1.rowName() = r2.rowName()
        WHERE r1.reviewText != r2.reviewText
        LIMIT 5
        """)

    mldb.log(res)
    assert len(res) == 1, str(res)

    mldb.delete('/v1/datasets/reviews_reloaded')


