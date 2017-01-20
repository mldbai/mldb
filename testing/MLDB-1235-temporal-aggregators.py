#
# MLDBFB-1235-temporal-aggregators.py
# 2016-02-04
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
# add this line to testing.mk:
# $(eval $(call mldb_unit_test,MLDBFB-336-sample_test.py,,manual))
#

import unittest

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
    
    def test_temporal_avg_on_row(self):
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

    def test_as_issue(self):
        """MLDBFB-415 temporal_min({*}) AS * issue"""
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row("user1", [["behA",1,0]])
        ds.commit()

        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : 'SELECT temporal_min({*}) AS * FROM ds',
                'outputDataset' : {
                    'id' : 'outDs',
                    'type' : 'sparse.mutable',
                },
                'runOnCreation' : True
            }
        })

        res = mldb.query("SELECT * FROM outDs")
        self.assertTableResultEquals(res, [
            ["_rowName", "behA"],
            ["user1", 1]
        ])

    def test_mldbfb_344_temporal_segfault(self):
        ds = mldb.create_dataset({'id' : 'ds2', 'type' : 'sparse.mutable'})
        ds.record_row('user1', [['behA', 1, 2]])
        ds.commit()

        for fct in ['count', 'sum', 'avg', 'min', 'max', 'latest', 'earliest']:
            self.assertTableResultEquals(
                mldb.query("""
                SELECT temporal_{}(behC) FROM ds2
                """.format(fct)), [
                    [
                        "_rowName",
                        "temporal_{}(behC)".format(fct)
                    ],
                    [
                        "user1",
                        None
                    ]
                ]
            )

    def test_mldbfb_516_aggregator_incorrect_with_join(self):
        ds = mldb.create_dataset({
            'id' : 'ds516',
            'type' : 'sparse.mutable'
        })
        ds.record_row('user3', [['behA', 1, 11], ['conv', 1, 70],
                                ['behB', 1, 14], ['behA', 1, 14]])
        ds.commit()

        ds = mldb.create_dataset({
            'id' : 'conv',
            'type' : 'sparse.mutable'
        })
        ds.record_row('user3', [['ts', 70, 0]])
        ds.commit()

        res = mldb.query("""
            SELECT temporal_count({ds516.*}) AS *
            FROM ds516
        """)
        mldb.log(res)
        self.assertTableResultEquals(res, [
            ['_rowName', 'behA', 'behB', 'conv'],
            ['user3', 2, 1, 1]
        ])

        res = mldb.query("""
            SELECT temporal_count({ds516.* as *}) AS *
            FROM ds516 INNER JOIN conv ON ds516.rowName() = conv.rowName()
        """)
        mldb.log(res)
        self.assertTableResultEquals(res, [
            ['_rowName', 'behA', 'behB', 'conv'],
            ['[user3]-[user3]', 2, 1, 1]
        ])
        mldb.log(res)
    
    def test_mldbfb_520_column_expr(self):
        """
        temporal_earliest doesn't yield correct result when used with
        COLUMN EXPR and wildcard expression
        """
        ds = mldb.create_dataset({
            'id' : 'mldbfb520_column_expr',
            'type' : 'sparse.mutable'
        })
        ds.record_row('user1', [['behA', 1, 1], ['behA', 1, 2], ['behA', 1, 3],
                                ['behB', 1, 9], ['behC', 1, 8]])
        ds.commit()

        query = """
            SELECT temporal_earliest({behA, behB}) AS * FROM mldbfb520_column_expr
        """
        res = mldb.get('/v1/query', q=query)

        expected = [{
            "rowName": "user1",
            "columns": [
                ["behA", 1, "1970-01-01T00:00:01Z"],
                ["behB", 1, "1970-01-01T00:00:09Z"]
            ]
        }]
        mldb.log(res)
        self.assertFullResultEquals(res.json(), expected)

        expected2 = [{
            "rowName": "user1",
            "columns": [
                ["behA", 1, "1970-01-01T00:00:01Z"],
                ["behA", 1, "1970-01-01T00:00:02Z"],
                ["behA", 1, "1970-01-01T00:00:03Z"],
                ["behB", 1, "1970-01-01T00:00:09Z"]
            ]
        }]
        query = """
            SELECT COLUMN EXPR (WHERE columnName() IN ('behA', 'behB'))
            FROM mldbfb520_column_expr
        """
        res = mldb.get('/v1/query', q=query)
        mldb.log(res)
        self.assertFullResultEquals(res.json(), expected2)

        query = """
            SELECT temporal_earliest({
                COLUMN EXPR (WHERE columnName() IN ('behA', 'behB'))
            }) AS * FROM mldbfb520_column_expr
        """
        res = mldb.get('/v1/query', q=query)
        mldb.log(res)
        self.assertFullResultEquals(res.json(), expected)

        query = """
            SELECT temporal_earliest({
                COLUMN EXPR (WHERE columnName() NOT IN ('behC'))
            }) AS * FROM mldbfb520_column_expr
        """
        res = mldb.get('/v1/query', q=query)
        self.assertFullResultEquals(res.json(), expected)

        expected = [{
            "rowName": "user1",
            "columns": [
                ["behA", 1, "1970-01-01T00:00:01Z"],
                ["behB", 1, "1970-01-01T00:00:09Z"],
                ["behC", 1, "1970-01-01T00:00:08Z"]
            ]
        }]
        query = """
            SELECT temporal_earliest({beh*}) AS * FROM mldbfb520_column_expr
        """
        res = mldb.get('/v1/query', q=query)
        mldb.log(res)
        self.assertFullResultEquals(res.json(), expected)

        expected = [{
            "rowName": "user1",
            "columns": [
                ["behA", 1, "1970-01-01T00:00:03Z"],
                ["behB", 1, "1970-01-01T00:00:09Z"],
                ["behC", 1, "1970-01-01T00:00:08Z"]
            ]
        }]
        query = """
            SELECT temporal_latest({beh*}) AS * FROM mldbfb520_column_expr
        """
        res = mldb.get('/v1/query', q=query)
        mldb.log(res)
        self.assertFullResultEquals(res.json(), expected)


    def test_mldbfb_520_sql_query(self):
        """
        temporal_earliest doesn't yield correct result when used with
        sql.query.
        """
        ds = mldb.create_dataset({
            'id' : 'mldbfb520_sql_query',
            'type' : 'sparse.mutable'
        })
        ds.record_row('user1', [['behA', 1, 1], ['behA', 1, 2], ['behA', 1, 3],
                                ['behB', 1, 9], ['behC', 1, 8]])
        ds.commit()

        query = """
            SELECT temporal_earliest({behA, behB}) AS * FROM mldbfb520_sql_query
        """
        res = mldb.get('/v1/query', q=query)

        expected = [{
            "rowName": "user1",
            "columns": [
                ["behA", 1, "1970-01-01T00:00:01Z"],
                ["behB", 1, "1970-01-01T00:00:09Z"]
            ]
        }]
        mldb.log(res)
        self.assertFullResultEquals(res.json(), expected)

        res = mldb.put('/v1/functions/early', {
            'type': 'sql.query',
            'params': {
                'query': 'SELECT temporal_earliest({beh*}) AS * FROM mldbfb520_sql_query'
            }
        })

        res = mldb.get('/v1/query', 
                       q = "SELECT early() AS * from mldbfb520_sql_query")
        expected = [{
            "rowName": "user1",
            "columns": [
                ["behA", 1, "1970-01-01T00:00:01Z"],
                ["behB", 1, "1970-01-01T00:00:09Z"],
                ["behC", 1, "1970-01-01T00:00:08Z"]
            ]
        }]
        mldb.log(res)
        self.assertFullResultEquals(res.json(), expected)


        res = mldb.put('/v1/functions/late', {
            'type': 'sql.query',
            'params': {
                'query': 'SELECT temporal_latest({beh*}) AS * FROM mldbfb520_sql_query'
            }
        })

        res = mldb.get('/v1/query', 
                       q = "SELECT late() AS * from mldbfb520_sql_query")
        

        expected = [{
            "rowName": "user1",
            "columns": [
                ["behA", 1, "1970-01-01T00:00:03Z"],
                ["behB", 1, "1970-01-01T00:00:09Z"],
                ["behC", 1, "1970-01-01T00:00:08Z"]
            ]
        }]
        mldb.log(res)
        self.assertFullResultEquals(res.json(), expected)

    @unittest.expectedFailure
    def test_mldbfb_520_join(self):
        """
        temporal_earliest doesn't yield correct result when used with
        join expressions.
        """
        ds = mldb.create_dataset({
            'id' : 'mldbfb520_join_left',
            'type' : 'sparse.mutable'
        })
        ds.record_row('user1', [['behA', 1, 1], ['behA', 1, 2], ['behA', 1, 3],
                                ['behB', 1, 9], ['behC', 1, 8]])
        ds.commit()

        ds = mldb.create_dataset({
            'id' : 'mldbfb520_join_right',
            'type' : 'sparse.mutable'
        })
        ds.record_row('user1', [['behD', 1, 1], ['behD', 1, 2], ['behD', 1, 3],
                                ['behB', 1, 9]])
        ds.commit()

        query = """
            SELECT temporal_earliest({
            COLUMN EXPR (WHERE columnName() IN ('l.behA', 'l.behB', 'r.behD'))}) AS * 
            FROM mldbfb520_join_left AS l
            INNER JOIN mldbfb520_join_right as r
            ON l.behB = r.behB
        """
        res = mldb.get('/v1/query', q=query)

        expected = [{
            "rowName": "[user1]-[user1]",
            "columns": [
                ["l.behA", 1, "1970-01-01T00:00:01Z"],
                ["l.behB", 1, "1970-01-01T00:00:09Z"],
                ["r.behD", 1, "1970-01-01T00:00:01Z"]
            ]
        }]
        mldb.log(res)
        self.assertFullResultEquals(res.json(), expected)

        query = """
            SELECT temporal_latest({
            COLUMN EXPR (WHERE columnName() IN ('l.behA', 'l.behB', 'r.behD'))}) AS * 
            FROM mldbfb520_join_left AS l
            INNER JOIN mldbfb520_join_right as r
            ON l.behB = r.behB
        """
        res = mldb.get('/v1/query', q=query)

        expected = [{
            "rowName": "[user1]-[user1]",
            "columns": [
                ["l.behA", 1, "1970-01-01T00:00:03Z"],
                ["l.behB", 1, "1970-01-01T00:00:09Z"],
                ["r.behD", 1, "1970-01-01T00:00:03Z"]
            ]
        }]
        mldb.log(res)
        self.assertFullResultEquals(res.json(), expected)
       
mldb.run_tests()
