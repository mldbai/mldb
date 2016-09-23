#
# union_dataset_test.py
# Francois-Michel L'Heureux, 2016-09-20
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class UnionDatasetTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds1', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['colA', 'A', 1]])
        ds.commit()

        ds = mldb.create_dataset({'id' : 'ds2', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['colB', 'B', 1]])
        ds.commit()

        ds = mldb.create_dataset({'id' : 'ds3', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['colA', 'AA', 1], ['colB', 'BB', 1]])
        ds.record_row('row2', [['colA', 'A', 1], ['colC', 'C', 1]])
        ds.commit()

    def test_dataset(self):
        mldb.put('/v1/datasets/union_ds', {
            'type' : 'union',
            'params' : {
                'datasets' : [{'id' : 'ds1'}, {'id' : 'ds2'}]
            }
        })

        res = mldb.query("SELECT colA, colB FROM union_ds ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB'],
            ['[]-[row1]', None, 'B'],
            ['[row1]-[]', 'A', None]
        ])

        res = mldb.query("SELECT * FROM union_ds ORDER BY rowName() LIMIT 1")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colB'],
            ['[]-[row1]', 'B']
        ])

        res = mldb.query("SELECT * FROM union_ds ORDER BY rowName() OFFSET 1")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA'],
            ['[row1]-[]', 'A']
        ])

        mldb.put('/v1/datasets/union_ds2', {
            'type' : 'union',
            'params' : {
                'datasets' : [{'id' : 'ds3'}, {'id' : 'ds3'}]
            }
        })
        res = mldb.query(
            "SELECT colA, colB, colC FROM union_ds2 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB', 'colC'],
            ['[]-[row1]', 'AA', 'BB', None],
            ['[]-[row2]', 'A', None, 'C'],
            ['[row1]-[]', 'AA', 'BB', None],
            ['[row2]-[]', 'A', None, 'C']
        ])

    def test_query(self):
        res = mldb.query(
            "SELECT colA, colB FROM union(ds1, ds2) ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB'],
            ['[]-[row1]', None, 'B'],
            ['[row1]-[]', 'A', None]
        ])

        res = mldb.query(
            "SELECT * FROM union(ds1, ds2) ORDER BY rowName() LIMIT 1")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colB'],
            ['[]-[row1]', 'B']
        ])

        res = mldb.query(
            "SELECT * FROM union(ds1, ds2) ORDER BY rowName() OFFSET 1")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA'],
            ['[row1]-[]', 'A']
        ])

        res = mldb.query(
            "SELECT colA, colB, colC FROM union(ds3, ds3) ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB', 'colC'],
            ['[]-[row1]', 'AA', 'BB', None],
            ['[]-[row2]', 'A', None, 'C'],
            ['[row1]-[]', 'AA', 'BB', None],
            ['[row2]-[]', 'A', None, 'C']
        ])

    # TODO MLDB-1958
    @unittest.expectedFailure
    def test_equivalent_query(self):
        res = mldb.query("""
            SELECT * FROM (SELECT * FROM ds1 ) AS s1
            OUTER JOIN (SELECT * FROM ds2) AS s2 ON false
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB'],
            ['[]-[row1]', None, 'B'],
            ['[row1]-[]', 'A', None]
        ])

if __name__ == '__main__':
    mldb.run_tests()
