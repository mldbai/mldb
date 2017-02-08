#
# MLDB-2077_merge_single_ds.py
# Francois-Michel L'Heureux, 2016-12-07
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2077MergeSingleDs(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('1', [['colA', 'A', 0]])
        ds.commit()

    def test_direct_query(self):
        res = mldb.query("SELECT * FROM merge(ds)")
        mldb.log(res)
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA'],
            ['1', 'A']
        ])

    def test_query_on_materialized_ds(self):
        mldb.put('/v1/datasets/mat', {
            'type' : 'merged',
            'params' : {
                'datasets' : [{'id' : 'ds'}]
            }
        })

        res = mldb.query("SELECT * FROM mat")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA'],
            ['1', 'A']
        ])

if __name__ == '__main__':
    mldb.run_tests()
