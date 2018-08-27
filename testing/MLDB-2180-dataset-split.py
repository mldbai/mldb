#
# MLDB-2180-dataset-split.py
# Mathieu Marquis Bolduc, 2017-04-03
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

from mldb import mldb, MldbUnitTest, ResponseException

class Mldb2180DatasetSplitTests(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds1', 'type' : 'sparse.mutable'})
        for i in range(4):
            val = 'x' if i < 2 else 'y'
            ds.record_row('%d' % i, [[val, 1, 0]])
        ds.commit()

        ds2 = mldb.create_dataset({'id' : 'ds2', 'type' : 'sparse.mutable'})
        for i in range(20):
            val = 'x' if i < 16 else 'y'
            ds2.record_row('%d' % i, [[val, 1, 0]])
        ds2.commit()

        ds2 = mldb.create_dataset({'id' : 'ds3', 'type' : 'sparse.mutable'})
        for i in range(20):
            val = 'x' if i < 10 else 'y'
            ds2.record_row('%d' % i, [[val, 1, 0]])
        ds2.commit()

        ds2 = mldb.create_dataset({'id' : 'ds4', 'type' : 'sparse.mutable'})
        #10/24 x, 15/24 y, 9 / 24 z
        for i in range(24):
            if i < 5:
                ds2.record_row('%d' % i, [['x', 1, 0]])
            elif i < 8:
                ds2.record_row('%d' % i, [['x', 1, 0], ['y', 1, 0]])
            elif i < 10:
                ds2.record_row('%d' % i, [['x', 1, 0], ['y', 1, 0], ['z', 1, 0]])
            elif i < 17:
                ds2.record_row('%d' % i, [['y', 1, 0]])
            elif i < 20:
                ds2.record_row('%d' % i, [['y', 1, 0], ['z', 1, 0]])
            else:
                ds2.record_row('%d' % i, [['z', 1, 0]])
        ds2.commit()

        ds = mldb.create_dataset({'id' : 'ds5', 'type' : 'sparse.mutable'})
        for i in range(4):
            val = 'x' if i < 3 else 'y'
            ds.record_row('%d' % i, [[val, 1, 0]])
        ds.commit()

    #Test that we try to represent every label in every dataset, regardless of distribution

    def test_spread(self):
        mldb.put("/v1/procedures/split", {
            "type": "split",
            "params": {
                "labels": "SELECT * FROM ds1",
                "reproducible": True,
                "splits": [0.8, 0.2],
                "outputDatasets": [{ "id": "ds_train",
                                   "type": "sparse.mutable" },
                                   { "id": "ds_test",
                                   "type": "sparse.mutable" }],
            }
        })   

        res1 = mldb.query("SELECT * FROM ds_train ORDER BY rowName() DESC")
        res2 = mldb.query("SELECT * FROM ds_test ORDER BY rowName() DESC")

        self.assertEqual(res1, [["_rowName", "y", "x"],
                                 ["3", 1, None ],
                                 ["0", None, 1 ]])

        self.assertEqual(res2, [["_rowName", "y", "x"],
                                 ["2", 1, None ],
                                 ["1", None, 1 ]])

    def test_testnointersection(self):
        mldb.put("/v1/procedures/split", {
            "type": "split",
            "params": {
                "reproducible": True,
                "labels": "SELECT * FROM ds2",
                "splits": [0.8, 0.2],
                "outputDatasets": [{ "id": "ds_train",
                                   "type": "sparse.mutable" },
                                   { "id": "ds_test",
                                   "type": "sparse.mutable" }],
            }
        })   

        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_train", format='atom').json()
        self.assertEqual(16, n)
        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_test", format='atom').json()
        self.assertEqual(4, n)

        res1 = mldb.query("SELECT sum({*}) FROM ds_train")
        res2 = mldb.query("SELECT sum({*}) FROM ds_test")

        self.assertEqual(res1, [["_rowName", "sum({*}).x", "sum({*}).y"],
                                 ["[]", 13, 3 ]])

        self.assertEqual(res2, [["_rowName", "sum({*}).x", "sum({*}).y"],
                                 ["[]", 3, 1 ]])

        mldb.put("/v1/procedures/split", {
            "type": "split",
            "params": {
                "reproducible": True,
                "labels": "SELECT * FROM ds3",
                "splits": [0.8, 0.2],
                "outputDatasets": [{ "id": "ds_train",
                                   "type": "sparse.mutable" },
                                   { "id": "ds_test",
                                   "type": "sparse.mutable" }],
            }
        })   

        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_train", format='atom').json()
        self.assertEqual(16, n)
        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_test", format='atom').json()
        self.assertEqual(4, n)

        res1 = mldb.query("SELECT sum({*}) FROM ds_train")
        res2 = mldb.query("SELECT sum({*}) FROM ds_test")

        self.assertEqual(res1, [["_rowName", "sum({*}).x", "sum({*}).y"],
                                 ["[]", 8, 8 ]])

        self.assertEqual(res2, [["_rowName", "sum({*}).x", "sum({*}).y"],
                                 ["[]", 2, 2 ]])

    def test_testintersection(self):

        mldb.put("/v1/procedures/split", {
            "type": "split",
            "params": {
                "reproducible": True,
                "labels": "SELECT * FROM ds4",
                "splits": [0.8, 0.2],
                "foldImportance" : 1.0,
                "outputDatasets": [{ "id": "ds_train",
                                   "type": "sparse.mutable" },
                                   { "id": "ds_test",
                                   "type": "sparse.mutable" }],
            }
        })   

        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_train", format='atom').json()
        self.assertEqual(19, n)
        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_test", format='atom').json()
        self.assertEqual(5, n)

        res1 = mldb.query("SELECT sum({*}) FROM ds_train")
        res2 = mldb.query("SELECT sum({*}) FROM ds_test")

        self.assertEqual(res1, [["_rowName", "sum({*}).x", "sum({*}).y", "sum({*}).z"],
                                 ["[]", 8, 11, 7 ]])

        self.assertEqual(res2, [["_rowName", "sum({*}).x", "sum({*}).y", "sum({*}).z"],
                                 ["[]", 2, 4, 2 ]])

        mldb.put("/v1/procedures/split", {
            "type": "split",
            "params": {
                "reproducible": True,
                "labels": "SELECT * FROM ds4",
                "splits": [0.8, 0.2],
                "foldImportance" : 5.0,
                "outputDatasets": [{ "id": "ds_train",
                                   "type": "sparse.mutable" },
                                   { "id": "ds_test",
                                   "type": "sparse.mutable" }],
            }
        })   

        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_train", format='atom').json()
        self.assertEqual(19, n)
        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_test", format='atom').json()
        self.assertEqual(5, n)

        res1 = mldb.query("SELECT sum({*}) FROM ds_train")
        res2 = mldb.query("SELECT sum({*}) FROM ds_test")

        self.assertEqual(res1, [["_rowName", "sum({*}).x", "sum({*}).y", "sum({*}).z"],
                                 ["[]", 8, 11, 6 ]])

        self.assertEqual(res2, [["_rowName", "sum({*}).x", "sum({*}).y", "sum({*}).z"],
                                 ["[]", 2, 4, 3 ]])

    def test_threesplits(self):

        mldb.put("/v1/procedures/split", {
            "type": "split",
            "params": {
                "reproducible": True,
                "labels": "SELECT * FROM ds4",
                "splits": [0.8, 0.1, 0.1],
                "foldImportance" : 1.0,
                "outputDatasets": [{ "id": "ds_train",
                                   "type": "sparse.mutable" },
                                   { "id": "ds_test",
                                   "type": "sparse.mutable" },
                                   { "id": "ds_validate",
                                   "type": "sparse.mutable" }],
            }
        })   

        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_train", format='atom').json()
        self.assertEqual(19, n)
        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_test", format='atom').json()
        self.assertEqual(2, n)
        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_validate", format='atom').json()
        self.assertEqual(3, n)

        res1 = mldb.query("SELECT sum({*}) FROM ds_train")
        res2 = mldb.query("SELECT sum({*}) FROM ds_test")
        res3 = mldb.query("SELECT sum({*}) FROM ds_validate")

        self.assertEqual(res1, [["_rowName", "sum({*}).x", "sum({*}).y", "sum({*}).z"],
                                 ["[]", 8, 11, 7 ]])

        self.assertEqual(res2, [["_rowName", "sum({*}).x", "sum({*}).y", "sum({*}).z"],
                                 ["[]", 1, 2, 1 ]])

        self.assertEqual(res2, [["_rowName", "sum({*}).x", "sum({*}).y", "sum({*}).z"],
                                 ["[]", 1, 2, 1 ]])

    def test_incomplete(self):
        res = mldb.put("/v1/procedures/split", {
            "type": "split",
            "params": {
                "reproducible": True,
                "labels": "SELECT * FROM ds5",
                "splits": [0.8, 0.2],
                "outputDatasets": [{ "id": "ds_train",
                                   "type": "sparse.mutable" },
                                   { "id": "ds_test",
                                   "type": "sparse.mutable" }],
            }
        })

        self.assertEqual(res.json()["status"]["firstRun"]["status"]["incompleteLabels"], 
                          ["y"])

        res1 = mldb.query("SELECT sum({*}) FROM ds_train")
        res2 = mldb.query("SELECT sum({*}) FROM ds_test")

        self.assertEqual(res1, [["_rowName", "sum({*}).x", "sum({*}).y"],
                                 ["[]", 2, 1]])

        self.assertEqual(res2, [["_rowName", "sum({*}).x"],
                                 ["[]", 1 ]])

    def test_errors(self):

        with self.assertRaisesRegex(ResponseException, 
                                     "Number of splits requested is different than the number of datasets provided"):
            res = mldb.put("/v1/procedures/split", {
                "type": "split",
                "params": {
                    "reproducible": True,
                    "labels": "SELECT * FROM ds5",
                    "splits": [0.8, 0.2, 0.3],
                    "outputDatasets": [{ "id": "ds_train",
                                       "type": "sparse.mutable" },
                                       { "id": "ds_test",
                                       "type": "sparse.mutable" }],
                }
            })

        with self.assertRaisesRegex(ResponseException, 
                                     "Insufficient number of splits"):
            res = mldb.put("/v1/procedures/split", {
                "type": "split",
                "params": {
                    "reproducible": True,
                    "labels": "SELECT * FROM ds5",
                    "splits": [0.8],
                    "outputDatasets": [{ "id": "ds_train",
                                       "type": "sparse.mutable" }],
                }
            })

        with self.assertRaisesRegex(ResponseException, 
                                     "Sum of split factors does not approximate to 1.0"):
            res = mldb.put("/v1/procedures/split", {
                "type": "split",
                "params": {
                    "reproducible": True,
                    "labels": "SELECT * FROM ds5",
                    "splits": [0.8, 0.1],
                    "outputDatasets": [{ "id": "ds_train",
                                       "type": "sparse.mutable" },
                                       { "id": "ds_test",
                                       "type": "sparse.mutable" }],
                }
            })


if __name__ == '__main__':
    request.set_return(mldb.run_tests())
