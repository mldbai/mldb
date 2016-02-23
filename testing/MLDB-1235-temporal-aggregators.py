#
# MLDBFB-1235-temporal-aggregators.py
# 2016-02-04
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

# add this line to testing.mk:
# $(eval $(call mldb_unit_test,MLDBFB-336-sample_test.py,,manual))


import unittest
import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

class TemporalTest(MldbUnitTest):

    sometime = '2016-01-02T12:23:34Z'
    before = '2016-01-01T12:23:34Z'
    after = '2016-01-03T12:23:34Z'

    @classmethod
    def setUpClass(self):
        # column values at three different times
        ds = mldb.create_dataset({
            'type': 'sparse.mutable',
            'id': 'dataset'})

        for i in xrange(1, 3):
            ds.record_row('row_' + str(i),
                           [['x', -i, TemporalTest.before], ['y', i, TemporalTest.before],
                            ['x', 0, TemporalTest.sometime], ['y', 0, TemporalTest.sometime],
                            ['x', i+1, TemporalTest.after], ['y', -i-1, TemporalTest.after]])
        ds.commit()

    def test_min_returns_last_event(self):
        # expressions are evaluated at latest time
        resp = mldb.query('select min(x) as min_x from dataset order by rowName()')
        mldb.log(resp)

        self.assertTableResultEquals(resp,
            [
                ["_rowName", "min_x"],
                ["[]",  2 ]
            ]
        )

    def test_temporal_earliest_on_column(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_earliest(x) as t_earliest_x from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "t_earliest_x", -1, TemporalTest.before ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "t_earliest_x", -2, TemporalTest.before ]
                    ]
                }
            ]
        )

    def test_temporal_earliest_on_row(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_earliest({*}) as * from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "x", -1, TemporalTest.before ],
                        [ "y", 1, TemporalTest.before ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "x", -2, TemporalTest.before ],
                        [ "y", 2, TemporalTest.before ]
                    ]
                }
            ]
        )


    def test_temporal_latest_on_column(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_latest(x) as t_latest_x from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "t_latest_x", 2, TemporalTest.after ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "t_latest_x", 3, TemporalTest.after ]
                    ]
                }
            ]
        )
      

    def test_temporal_latest_on_row(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_latest({*}) as * from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "x", 2, TemporalTest.after ],
                        [ "y", -2, TemporalTest.after ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "x", 3, TemporalTest.after ],
                        [ "y", -3, TemporalTest.after ]
                    ]
                }
            ]
        )

    def test_temporal_min_on_column(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_min(x) from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "temporal_min(x)", -1, TemporalTest.before ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "temporal_min(x)", -2, TemporalTest.before ]
                    ]
                }
            ]
        )

    def test_temporal_min_on_row(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_min({*}) as * from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "x", -1, TemporalTest.before ],
                        [ "y", -2, TemporalTest.after ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "x", -2, TemporalTest.before ],
                        [ "y", -3, TemporalTest.after ]
                    ]
                }
            ]
        )

 
    def test_temporal_max_on_column(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_max(x) as max from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "max", 2, TemporalTest.after ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "max", 3, TemporalTest.after ]
                    ]
                }
            ]
        )

    def test_temporal_max_on_row(self):

        resp = mldb.get('/v1/query',
                        q = 'select temporal_max({*}) as * from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "x", 2, TemporalTest.after ],
                        [ "y", 1, TemporalTest.before ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "x", 3, TemporalTest.after ],
                        [ "y", 2, TemporalTest.before ]
                    ]
                }
            ]
        )

    def test_temporal_count_on_column(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_count(x) from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "temporal_count(x)", 3, TemporalTest.after ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "temporal_count(x)", 3, TemporalTest.after ]
                    ]
                }
            ]
        )

    def test_temporal_count_on_row(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_count({*}) as * from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "x", 3, TemporalTest.after ],
                        [ "y", 3, TemporalTest.after ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "x", 3, TemporalTest.after ],
                        [ "y", 3, TemporalTest.after ]
                    ]
                }
            ]
        )

    def test_temporal_sum_on_column(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_sum(x) as sum from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "sum", 1, TemporalTest.after ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "sum", 1, TemporalTest.after ]
                    ]
                }
            ]
        )

    def test_temporal_sum_on_row(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_sum({*}) as * from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "x", 1, TemporalTest.after ],
                        [ "y", -1, TemporalTest.after ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "x", 1, TemporalTest.after ],
                        [ "y", -1, TemporalTest.after ]
                    ]
                }
            ]
        )

    def test_temporal_avg_on_column(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_avg(x) as avg from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "avg", 0.3333333333333333, TemporalTest.after ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "avg", 0.3333333333333333, TemporalTest.after ]
                    ]
                }
            ]
        )

    def test_temporal_sum_on_row(self):
        resp = mldb.get('/v1/query',
                        q = 'select temporal_avg({*}) as * from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [ "x", 0.3333333333333333, TemporalTest.after ],
                        [ "y", -0.3333333333333333, TemporalTest.after ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [ "x", 0.3333333333333333, TemporalTest.after ],
                        [ "y", -0.3333333333333333, TemporalTest.after ]
                    ]
                }
            ]
        )

mldb.run_tests()
