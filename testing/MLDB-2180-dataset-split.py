#
# MLDB-2180-dataset-split.py
# Mathieu Marquis Bolduc, 2017-04-03
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2180DatasetSplitTests(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds1', 'type' : 'sparse.mutable'})
        ds.record_row('0', [['x', 1, 0]])
        ds.record_row('1', [['x', 1, 0]])
        ds.record_row('2', [['y', 1, 0]])
        ds.record_row('3', [['y', 1, 0]])
        ds.commit()

        ds2 = mldb.create_dataset({'id' : 'ds2', 'type' : 'sparse.mutable'})
        ds2.record_row('0', [['x', 1, 0]])
        ds2.record_row('1', [['x', 1, 0]])
        ds2.record_row('2', [['x', 1, 0]])
        ds2.record_row('3', [['x', 1, 0]])
        ds2.record_row('4', [['x', 1, 0]])
        ds2.record_row('5', [['x', 1, 0]])
        ds2.record_row('6', [['x', 1, 0]])
        ds2.record_row('7', [['x', 1, 0]])
        ds2.record_row('8', [['x', 1, 0]])
        ds2.record_row('9', [['x', 1, 0]])
        ds2.record_row('10', [['x', 1, 0]])
        ds2.record_row('11', [['x', 1, 0]])
        ds2.record_row('12', [['x', 1, 0]])
        ds2.record_row('13', [['x', 1, 0]])
        ds2.record_row('14', [['x', 1, 0]])
        ds2.record_row('15', [['x', 1, 0]])
        ds2.record_row('16', [['y', 1, 0]])
        ds2.record_row('17', [['y', 1, 0]])
        ds2.record_row('18', [['y', 1, 0]])
        ds2.record_row('19', [['y', 1, 0]])
        ds2.commit()

        ds2 = mldb.create_dataset({'id' : 'ds3', 'type' : 'sparse.mutable'})
        ds2.record_row('0', [['x', 1, 0]])
        ds2.record_row('1', [['x', 1, 0]])
        ds2.record_row('2', [['x', 1, 0]])
        ds2.record_row('3', [['x', 1, 0]])
        ds2.record_row('4', [['x', 1, 0]])
        ds2.record_row('5', [['x', 1, 0]])
        ds2.record_row('6', [['x', 1, 0]])
        ds2.record_row('7', [['x', 1, 0]])
        ds2.record_row('8', [['x', 1, 0]])
        ds2.record_row('9', [['x', 1, 0]])
        ds2.record_row('10', [['y', 1, 0]])
        ds2.record_row('11', [['y', 1, 0]])
        ds2.record_row('12', [['y', 1, 0]])
        ds2.record_row('13', [['y', 1, 0]])
        ds2.record_row('14', [['y', 1, 0]])
        ds2.record_row('15', [['y', 1, 0]])
        ds2.record_row('16', [['y', 1, 0]])
        ds2.record_row('17', [['y', 1, 0]])
        ds2.record_row('18', [['y', 1, 0]])
        ds2.record_row('19', [['y', 1, 0]])
        ds2.commit()

        ds2 = mldb.create_dataset({'id' : 'ds4', 'type' : 'sparse.mutable'})
        #10/24 x, 15/24 y, 9 / 24 z
        ds2.record_row('0', [['x', 1, 0]])
        ds2.record_row('1', [['x', 1, 0]])
        ds2.record_row('2', [['x', 1, 0]])
        ds2.record_row('3', [['x', 1, 0]])
        ds2.record_row('4', [['x', 1, 0]])
        ds2.record_row('5', [['x', 1, 0], ['y', 1, 0]])
        ds2.record_row('6', [['x', 1, 0], ['y', 1, 0]])
        ds2.record_row('7', [['x', 1, 0], ['y', 1, 0]])
        ds2.record_row('8', [['x', 1, 0], ['y', 1, 0], ['z', 1, 0]])
        ds2.record_row('9', [['x', 1, 0], ['y', 1, 0], ['z', 1, 0]])
        ds2.record_row('10', [['y', 1, 0]])
        ds2.record_row('11', [['y', 1, 0]])
        ds2.record_row('12', [['y', 1, 0]])
        ds2.record_row('13', [['y', 1, 0]])
        ds2.record_row('14', [['y', 1, 0]])
        ds2.record_row('15', [['y', 1, 0]])
        ds2.record_row('16', [['y', 1, 0]])
        ds2.record_row('17', [['y', 1, 0], ['z', 1, 0]])
        ds2.record_row('18', [['y', 1, 0], ['z', 1, 0]])
        ds2.record_row('19', [['y', 1, 0], ['z', 1, 0]])
        ds2.record_row('20', [['z', 1, 0]])
        ds2.record_row('21', [['z', 1, 0]])
        ds2.record_row('22', [['z', 1, 0]])
        ds2.record_row('23', [['z', 1, 0]])
        ds2.commit()

    #Test that we try to represent every label in every dataset, regardless of distribution
    """
    def test_spread(self):
        mldb.put("/v1/procedures/split", {
            "type": "split",
            "params": {
                "labels": "SELECT * FROM ds1",
                "splits": [0.8, 0.2],
                "outputDatasets": [{ "id": "ds_train",
                                   "type": "sparse.mutable" },
                                   { "id": "ds_test",
                                   "type": "sparse.mutable" }],
            }
        })   

        res1 = mldb.query("SELECT * FROM ds_train")
        res2 = mldb.query("SELECT * FROM ds_test")

        self.assertEquals(res1, [["_rowName", "y", "x"],
                                 ["3", 1, None ],
                                 ["1", None, 1 ]])

        self.assertEquals(res2, [["_rowName", "y", "x"],
                                 ["2", 1, None ],
                                 ["0", None, 1 ]])

    def test_testnointersection(self):
        mldb.put("/v1/procedures/split", {
            "type": "split",
            "params": {
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

        self.assertEquals(res1, [["_rowName", "sum({*}).x", "sum({*}).y"],
                                 ["[]", 13, 3 ]])

        self.assertEquals(res2, [["_rowName", "sum({*}).x", "sum({*}).y"],
                                 ["[]", 3, 1 ]])

        mldb.put("/v1/procedures/split", {
            "type": "split",
            "params": {
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

        self.assertEquals(res1, [["_rowName", "sum({*}).x", "sum({*}).y"],
                                 ["[]", 8, 8 ]])

        self.assertEquals(res2, [["_rowName", "sum({*}).x", "sum({*}).y"],
                                 ["[]", 2, 2 ]])
    """
    def test_testintersection(self):

        mldb.put("/v1/procedures/split", {
            "type": "split",
            "params": {
                "labels": "SELECT * FROM ds4",
                "splits": [0.8, 0.2],
                "outputDatasets": [{ "id": "ds_train",
                                   "type": "sparse.mutable" },
                                   { "id": "ds_test",
                                   "type": "sparse.mutable" }],
            }
        })   

        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_train", format='atom').json()
        mldb.log(n)
        n = mldb.get('/v1/query', q="SELECT count(*) FROM ds_test", format='atom').json()
        mldb.log(n)

        mldb.log(mldb.query("SELECT sum({*}) FROM ds_train"))
        mldb.log(mldb.query("SELECT sum({*}) FROM ds_test"))

if __name__ == '__main__':
    mldb.run_tests()