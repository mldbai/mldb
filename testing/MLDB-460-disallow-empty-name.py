# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

import datetime
import json
import sys

# create a mutable beh dataset


def test_dataset(config):
    dataset = mldb.create_dataset(config)

    # non-empty row name and column name
    dataset.record_rows([["row_name", [["column_name", 5, datetime.datetime.now()]]]])

    dataset.record_rows([["row_name", [["\"\"", 4, datetime.datetime.now()]]]])
    dataset.record_row("\"\"", [["column_name", 5, datetime.datetime.now()]]) 

    try:
        dataset.record_rows([["row_name", [["", 4, datetime.datetime.now()]]]])
    except Exception, e:
        assert 'empty column names are not allowed' in str(e)

    try:
        dataset.record_row("", [["column_name", 5, datetime.datetime.now()]]) 
    except Exception, e:
        assert 'empty row names are not allowed' in str(e)
        
    dataset.commit()

mutable_dataset_types = [
    {
        "type": 'beh.mutable',
        "id": 'beh',
        "params": {
            "dataFileUrl": 'file://tmp/MLDB-460.beh'
        }
    },
    {
        "type": 'sparse.mutable',
        "id": 'sparse'
    },
    {
        "type": 'sqliteSparse',
        "id": 'sqlitebeh',
        "params": {
            "dataFileUrl": 'file://tmp/MLDB-460.sqlite'
        }
    }
]
    
for config in mutable_dataset_types:
    test_dataset(config)

mldb.script.set_return("success")
