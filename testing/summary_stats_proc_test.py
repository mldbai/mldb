#
# summary_stats_proc_test.py
# Francois-Michel L Heureux, 2016-07-04
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb)  # noqa

class SummaryStatsProcTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
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

    def test_it(self):
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
        res = mldb.query("SELECT * FROM output order by rowPath()")
        mldb.log(res)
        self.assertTableResultEquals(res, [
            ["_rowName", "value.data_type", "value.num_null",
             "value.num_unique", "value.max", "value.avg", "value.min",
             "value.1st_quartile", "value.median", "value.3rd_quartile",
             "value.most_frequent_items.banane",
             "value.most_frequent_items.20", "value.most_frequent_items.2",
             "value.most_frequent_items.1", "value.stddev",
             "value.most_frequent_items.10",
             "value.most_frequent_items.patate"],

            ["colA", "number", 0, 2, 10, 4, 1, 1, 1, 10, None, None, None, 2,
             5.196152422706632, 1, None],

            ["colB", "number", 2, 1, 2, 2, 2, 2, 2, 2, None, None, 1, None,
             "NaN", None, None],

            ["colC", "number", 2, 1, 20, 20, 20, 20, 20, 20, None, 1, None,
              None, "NaN", None, None],

            ["colTxt", "categorical", 1, 2, None, None, None, None, None,
             None, 1, None, None, None, None, None, 1]


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
            ["_rowName", "value.1st_quartile", "value.3rd_quartile",
             "value.data_type", "value.max", "value.avg", "value.median",
             "value.min", "value.most_frequent_items.1", "value.num_null",
             "value.num_unique", "value.stddev"],
            ['"col.a"', 1, 1, "number", 1, 1, 1, 1, 1, 0, 1, "NaN"]
        ])

    def test_unexisting_col(self):
        mldb.post('/v1/procedures', {
            'type' : 'summary.statistics',
            'params' : {
                'runOnCreation' : True,
                'inputData' : 'SELECT unexisting FROM ds',
                'outputDataset' : {
                    'id' : 'output_unexisting_col_ds',
                    'type' : 'sparse.mutable'
                }
            }
        })

        res = mldb.query("SELECT * FROM output_unexisting_col_ds")
        self.assertTableResultEquals(res, [
            ["_rowName", "value.data_type", "value.num_null",
             "value.num_unique"],

            ["unexisting", "categorical", 3, 0]
        ])

    def test_invalid_select(self):
        def run_proc(input_data):
            mldb.post('/v1/procedures', {
                'type' : 'summary.statistics',
                'params' : {
                    'runOnCreation' : True,
                    'inputData' : input_data,
                    'outputDataset' : {
                        'id' : 'error',
                        'type' : 'sparse.mutable'
                    }
                }
            })
        msg = "is not a supported SELECT value expression for " \
              "summary.statistics"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            run_proc('SELECT coco AS * FROM ds')

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            run_proc('SELECT {a:1, b:2} FROM ds')

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            run_proc('SELECT colA + 1 FROM ds')

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            run_proc('SELECT {*} FROM ds')

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            run_proc('SELECT max(colA) FROM ds')

    def test_most_frequent(self):
        ds = mldb.create_dataset({
            'id' : 'most_freq_source',
            'type' : 'sparse.mutable'
        })

        row_num = 0
        class Counter(object):
            def __init__(self):
                self.num = 0

            def next(self):
                self.num += 1
                return self.num

        vals = {
            'a' : 5,
            'b' : 4,
            'c' : 3,
            'd' : 2,
            'e' : 1,
            'f' : 1,
            'g' : 1,
            'h' : 1,
            'i' : 1,
            'j' : 1,
            'k' : 1,
            'l' : 1,
            'm' : 1,
        }
        c = Counter()
        for k, count in vals.iteritems():
            for _ in xrange(count):
                ds.record_row(c.next(), [['col', k, 0]])

        ds.commit()

        mldb.post('/v1/procedures', {
            'type' : 'summary.statistics',
            'params' : {
                'runOnCreation' : True,
                'inputData' : "SELECT * FROM most_freq_source",
                'outputDataset' : {
                    'id' : 'most_freq_output',
                    'type' : 'sparse.mutable'
                }
            }
        })

        res = mldb.query("SELECT * FROM most_freq_output ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ["_rowName", "value.data_type", "value.most_frequent_items.a",
             "value.most_frequent_items.b", "value.most_frequent_items.c",
             "value.most_frequent_items.d", "value.most_frequent_items.h",
             "value.most_frequent_items.i", "value.most_frequent_items.j",
             "value.most_frequent_items.k", "value.most_frequent_items.l",
             "value.most_frequent_items.m", "value.num_null",
             "value.num_unique"],
            [ "col", "categorical", 5, 4, 3, 2, 1, 1, 1, 1, 1, 1, 0, 13]
        ])



if __name__ == '__main__':
    mldb.run_tests()
