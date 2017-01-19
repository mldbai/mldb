#
# MLDB-1921_merge_ds_strings.py
# Francois-Michel L'Heureux, 2016-09-30
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1921MergeDsStrings(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds1', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['A', 1, 0]])
        ds.commit()

        ds = mldb.create_dataset({'id' : 'ds2', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['B', 2, 0]])
        ds.commit()

    @unittest.expectedFailure
    def test_it(self):
        mldb.put('/v1/datasets/merged_ds', {
            'type' : 'merged',
            'params' : {
                'datasets' : ['ds1', 'ds2']

                # explicit way to define datasets
                #'datasets' : [{'id' : 'ds1'}, {'id' : 'ds2'}]
            }
        })

        res = mldb.query("SELECT A, B FROM merged_ds ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'A', 'B'],
            ['row1', 1, 2]
        ])

if __name__ == '__main__':
    mldb.run_tests()
