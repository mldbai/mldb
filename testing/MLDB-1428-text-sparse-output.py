#
# MLDB-1428-text-sparse-output.py
# Mathieu Bolduc, 2016-02-29
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa


import unittest
class HavingTest(unittest.TestCase):

    def test_having(self):

        res = mldb.put('/v1/procedures/import_reddit', { 
            "type": "import.text",  
            "params": { 
                "dataFileUrl": "http://files.figshare.com/1310438/reddit_user_posting_behavior.csv.gz",
                "delimiter": "",
                "quotechar": "",
                "select": "tokenize(lineText, {offset: 1, value: 1}) as *",
                'outputDataset': {'id': 'reddit', 'type': 'sparse.mutable'},
                'runOnCreation': True
            } 
        })

        mldb.log(res)

        res = mldb.get('/v1/query', q='SELECT * FROM reddit LIMIT 10')

        mldb.log(res)        

mldb.run_tests()
