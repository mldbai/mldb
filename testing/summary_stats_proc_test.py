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
        mldb.put('/v1/datasets/ds', {'type' : 'sparse.mutable'})
        mldb.post('/v1/datasets/ds/rows', {
            'rowName' : 'row1',
            'columns' : [
                        ['colA', 1, 0],
                        ['colB', 2, 0],
                        ['colTxt', 'patate', 0]
                    ]
            })
        print mldb.post('/v1/datasets/ds/rows', {
            'rowName' : 'row2',
            'columns' : [
                        ['colA', 10, 0],
                        ['colC', 20, 0],
                        ['colTxt', 'banane', 0]
                    ]
            })
        mldb.post('/v1/datasets/ds/commit')

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
            ["_rowName", "data_type", "max", "mean", "median", "min",
             "num_null", "num_unique", "quartile1", "quartile3", "std"],
            ["colC", "numerical", 20, 20, 0, 20, 1, 1, 0, 0, 0],
            ["colB", "numerical", 2, 2, 0, 2, 1, 1, 0, 0, 0],
            ["colA", "numerical", 10, 1, 0, 1, 0, 2, 0, 0, 0]
        ])

if __name__ == '__main__':
    mldb.run_tests()
