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
            ["_rowName", "value_data_type", "value_num_null",
             "value_num_unique", "value_max", "value_mean", "value_min",
             "value_1st_quartile", "value_median", "value_3rd_quartile",
             '"value_most_frequent_items.banane"',
             '"value_most_frequent_items.20"',
             '"value_most_frequent_items.2"',
             '"value_most_frequent_items.1"'],

            ["colTxt", "categorical", 1, 2, None, None, None, None, None,
             None, 1, None, None, None],

            ["colC", "number", 2, 1, 20, 20, 20, 20, 20, 20, None, 1, None,
             None],

            ["colB", "number", 2, 1, 2, 2, 2, 2, 2, 2, None, None, 1, None],
            ["colA", "number", 0, 2, 10, 4, 1, 1, 1, 10, None, None, None, 2]
        ])

    def test_dottest_col_names(self):
        ds = mldb.create_dataset({
            'id' : 'dotted_col_ds',
            'type' : 'sparse.mutable'
        })
        ds.record_row('row1', [['col.a', 1, 0]])
        ds.commit()

        mldb.post('/v1/procedures', {
            'type' : 'summary.statistics',
            'params' : {
                'runOnCreation' : True,
                'inputData' : 'SELECT * FROM dotted_col_ds',
                'outputDataset' : {
                    'id' : 'output_dotted_col_ds',
                    'type' : 'sparse.mutable'
                }
            }
        })
        res = mldb.query("SELECT * FROM output_dotted_col_ds")
        self.assertTableResultEquals(res, [
            ["_rowName", "value_1st_quartile", "value_3rd_quartile",
             "value_data_type", "value_max", "value_mean", "value_median",
             "value_min", '"value_most_frequent_items.1"', "value_num_null",
             "value_num_unique"],
            ['"""col.a"""', 1, 1, "number", 1, 1, 1, 1, 1, 0, 1]
        ])

if __name__ == '__main__':
    mldb.run_tests()
