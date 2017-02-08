#
# MLDB-1428-text-sparse-output.py
# Mathieu Bolduc, 2016-02-29
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
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

        res = mldb.query('SELECT * FROM iris ORDER BY rowName() limit 1')

        expected = [[
                        "_rowName",
                        "a",
                        "b",
                        "c",
                        "d",
                        "label"
                    ],
                    [u'1', 5.1, 3.5, 1.4, 0.2, u'Iris-setosa']]

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

        res = mldb.query('SELECT * FROM iris_ex ORDER BY rowName() limit 1')

        expected = [["_rowName", "a", "b", "d", "label" ],
                    [u'1', 5.1, 3.5, 0.2, u'Iris-setosa']]

        self.assertEqual(res, expected)

    def test_sparse_unknown_cols(self):

        res = mldb.put('/v1/procedures/import_reddit', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "https://public.mldb.ai/reddit.csv.gz",
                "delimiter": "",
                "quoteChar": "",
                "select": "tokenize(lineText, {offset: 1, value: 1}) as *",
                'outputDataset': {'id': 'reddit', 'type': 'sparse.mutable'},
                'runOnCreation': True
            } 
        })

        res = mldb.query('SELECT gonewild FROM reddit WHERE gonewild IS NOT NULL ORDER BY rowName() LIMIT 1')

        expected = [["_rowName","gonewild"],
                    ["100030",1]]
        self.assertEqual(res, expected)

    # MLDB-1513
    def test_missing_rows(self):
        ds = mldb.create_dataset({
            'type': 'tabular',
            'id': 'onechunk'})

        ds.record_row('1', [['x', 17, 0]])
        ds.record_row('2', [['x', 18, 0]])
        ds.record_row('3', [['x', 'banane', 0]])

        ds.commit()

        res = mldb.query("select * from onechunk where rowName() in ('1', '2', '3')")

        expected = [["_rowName", "x"],["1",17],["2",18],["3","banane"]]

        self.assertEqual(res, expected)

    def test_missing_rows_order(self):

        ds = mldb.create_dataset({
            'type': 'sparse.mutable',
            'id': 'onechunk2'})

        ds.record_row('1', [['x', 17, 0]])
        ds.record_row('2', [['x', 18, 0]])
        ds.record_row('3', [['x', 'banane', 0]])

        ds.commit()

        res = mldb.query("select * from onechunk2 where rowName() in ('1', '2', '3') order by rowName()")
        expected = [["_rowName", "x"],["1",17],["2",18],["3","banane"]]

        self.assertEqual(res, expected)

mldb.run_tests()
