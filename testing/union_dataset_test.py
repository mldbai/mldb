#
# union_dataset_test.py
# Francois-Michel L'Heureux, 2016-09-20
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class UnionDatasetTest(MldbUnitTest):  # noqa

    def test_it(self):
        ds = mldb.create_dataset({'id' : 'ds1', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['colA', 'A', 1]])
        ds.commit()

        ds = mldb.create_dataset({'id' : 'ds2', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['colB', 'B', 1]])
        ds.commit()

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

if __name__ == '__main__':
    mldb.run_tests()
