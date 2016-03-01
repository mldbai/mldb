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
                           [['x', -i, TemporalTest.before], ['y', -i, TemporalTest.before],
                            ['x', 0, TemporalTest.sometime], ['y', 0, TemporalTest.sometime],
                            ['x', i+1, TemporalTest.after], ['y', i+1, TemporalTest.after]])
        ds.commit()

    def test_min_returns_last_event(self):
        #all expressions are evaluated at latest time
        resp = mldb.query('select min(x) as min_x from dataset order by rowName()')
        mldb.log(resp)

        self.assertTableResultEquals(resp,
            [
                ["_rowName", "min_x"],
                ["[]",  2 ]
            ]
        )

    def test_temporal_earliest_returns_first_event(self):
        #temporal earliest on one column
        resp = mldb.get('/v1/query',
                        q = 'select temporal_earliest(x) as t_earliest_x from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [
                            "t_earliest_x",
                            -1,
                             TemporalTest.before
                        ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [
                            "t_earliest_x",
                            -2,
                             TemporalTest.before
                        ]
                    ]
                }
            ]
        )

    def test_temporal_earliest_on_rows(self):
        #temporal earliest on one column
        resp = mldb.get('/v1/query',
                        q = 'select temporal_earliest({*}) as * from dataset order by rowName()',
                        format = 'full').json()

        self.assertFullResultEquals(resp,
            [
                {
                    "rowName": "row_1",
                    "rowHash": "f156570c0871dbce",
                    "columns": [
                        [
                            "x",
                            -1,
                            "2016-01-01T12:23:34Z"
                        ],
                        [
                            "y",
                            -1,
                            "2016-01-01T12:23:34Z"
                        ]
                    ]
                },
                {
                    "rowName": "row_2",
                    "rowHash": "0ea93be3f94d4404",
                    "columns": [
                        [
                            "x",
                            -2,
                            "2016-01-01T12:23:34Z"
                        ],
                        [
                            "y",
                            -2,
                            "2016-01-01T12:23:34Z"
                        ]
                    ]
                }
            ]
        )


mldb.run_tests()
