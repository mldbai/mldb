#
# MLDB-1428-text-sparse-output.py
# Mathieu Bolduc, 2016-02-29
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa


import unittest
class ImportTextToSparseTest(unittest.TestCase):

    def test_sparse(self):
        res = mldb.put('/v1/procedures/import_reddit', { 
            "type": "import.text",  
            "params": { 
                "dataFileUrl": "file://mldb/testing/dataset/iris.data",
                'encoding' : 'latin1',
                "select": "*",
                'outputDataset': {'id': 'iris', 'type': 'sparse.mutable'},
                 'headers': ["a", "b", "c", "d", "label"],
                'runOnCreation': True
            } 
        })

        res = mldb.query('SELECT * FROM iris limit 1')

        expected = [["_rowName",
                    "a",
                    "b",
                    "c",
                    "d",
                    "label"],
                    [
                        "97",
                        5.7,
                        2.9,
                        4.2,
                        1.3,
                        "Iris-versicolor"
                    ]]

        self.assertEqual(res, expected)

    def test_sparse_excluding(self):
        res = mldb.put('/v1/procedures/import_reddit', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/iris.data",
                'encoding' : 'latin1',
                "select": "* excluding(c)",
                'outputDataset': {'id': 'iris_ex', 'type': 'sparse.mutable'},
                 'headers': ["a", "b", "c", "d", "label"],
                'runOnCreation': True
            } 
        })

        res = mldb.query('SELECT * FROM iris_ex limit 1')

        expected = [["_rowName", "a", "b", "d","label"],
                    ["97", 5.7, 2.9, 1.3, "Iris-versicolor"]]

        self.assertEqual(res, expected)

    def test_sparse_unknown_cols(self):

        res = mldb.put('/v1/procedures/import_reddit', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "https://s3.amazonaws.com/public.mldb.ai/reddit.csv.gz",
                "delimiter": "",
                "quotechar": "",
                "select": "tokenize(lineText, {offset: 1, value: 1}) as *",
                'outputDataset': {'id': 'reddit', 'type': 'sparse.mutable'},
                'runOnCreation': True
            } 
        })

      #  mldb.log(res)

        res = mldb.query('SELECT gonewild FROM reddit LIMIT 1')

        expected = [["_rowName","gonewild"],
                    ["471242",1]]
        self.assertEqual(res, expected)

mldb.run_tests()
