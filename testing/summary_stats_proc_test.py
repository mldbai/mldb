#
# summary_stats_proc_test.py
# Francois-Michel L Heureux, 2016-07-04
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class SummaryStatsProcTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        pass

    def test_it(self):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [
            ['colA', 1, 0],
            ['colB', 2, 0],
            ['colTxt', 'patate', 0]
        ])
        ds.record_row('row2', [
            ['colA', 10, 0],
            ['colC', 20, 0],
            ['colTxt', 'banane', 0]
        ])
        ds.record_row('row3', [
            ['colA', 1, 1]
        ])
        ds.commit()

        mldb.post('/v1/procedures', {
            'type' : 'summary.statistics',
            'params' : {
                'runOnCreation' : True,
                'inputData' : 'SELECT * FROM ds',
                'outputDataset' : {
                    'id' : 'output',
                    'type' : 'sparse.mutable'
                }
            }
        })
        res = mldb.query("SELECT * FROM output")
        self.assertTableResultEquals(res, [
            ["_rowName", "data_type", "num_null", "num_unique", "max", "mean", "min"],
            ["colTxt", "categorical", 1, 2, None, None, None],
            ["colC", "number", 2, 1, 20, 20, 20],
            ["colB", "number", 2, 1, 2, 2, 2],
            ["colA", "number", 0, 2, 10, 4, 1]
        ])

if __name__ == '__main__':
    mldb.run_tests()
