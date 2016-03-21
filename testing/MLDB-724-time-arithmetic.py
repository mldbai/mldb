# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
# Test for MLDB-724; timestamp arithmetics

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class TimeArithmeticTest(MldbUnitTest):  

    ts = "2015-01-01"
    ts_plus_1d = "2015-01-02"
    ts_plus_2d = "2015-01-03"
    ts_plus_1month = "2015-02-01"

    @classmethod
    def setUpClass(self):
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'test',
        }

        dataset = mldb.create_dataset(dataset_config)
        
        dataset.record_row('imp_then_click_1d', [ ["imp", 0, self.ts], ["click", 0, self.ts_plus_1d] ])
        dataset.record_row('imp_then_click_1month', [ ["imp", 0, self.ts], ["click", 0, self.ts_plus_1month] ])
        dataset.record_row('click_then_imp_2d', [ ["imp", 0, self.ts_plus_2d], ["click", 0, self.ts] ])
    
        dataset.commit()
        
    def test_second_equivalence(self):
        self.assertTableResultEquals(
            mldb.query("select INTERVAL '10 s' = INTERVAL '10second' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )


        self.assertTableResultEquals(
            mldb.query("select INTERVAL '22S' = INTERVAL '22 SECOND' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

    def test_minute_equivalence(self):
        self.assertTableResultEquals(
            mldb.query("select INTERVAL '60 MINUTE' = INTERVAL '1H' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )


        self.assertTableResultEquals(
            mldb.query("select INTERVAL '10 minute' = INTERVAL '600second' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

    def test_hour_equivalence(self):
        self.assertTableResultEquals(
            mldb.query("select INTERVAL '2H' = INTERVAL '120m' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )


        self.assertTableResultEquals(
            mldb.query("select INTERVAL '2 hour' = INTERVAL '2 HOUR' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(
            mldb.query("select INTERVAL '24 H' = INTERVAL '1440 m' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(
            mldb.query("select INTERVAL '24 H' = INTERVAL '86400 s' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

    def test_day_equivalence(self):
        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1 d' = INTERVAL '1day' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1 D' = INTERVAL '1 DAY' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

    def test_week_equivalence(self):
        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1 w' = INTERVAL '7day' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1week' = INTERVAL '1 WEEK' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

    def test_month_equivalence(self):
        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1MONTH' = INTERVAL '1 month' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

    def test_year_equivalence(self):
        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1 year' = INTERVAL '12month' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1YEAR' = INTERVAL '1 Y' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

    def test_mixed_equivalence(self):
        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1Y2W' = INTERVAL '12MONTH14d' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1 day 5H' = INTERVAL '1d 18000 second' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  True ]
            ]
        )

    def test_not_equivalent(self):
        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1 day' = INTERVAL '24H' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  False ] # because of daylight saving
            ]
        )

        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1 month' = INTERVAL '30day' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  False ] # because months were not all created equal
            ]
        )

        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1 month' = INTERVAL '4 week' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  False ] # because months were not all created equal
            ]
        )

        self.assertTableResultEquals(
            mldb.query("select INTERVAL '1 year' = INTERVAL '365 day' as equal"),
            [
                ["_rowName", "equal"],
                [       "",  False ] # because of leap years
            ]
        )

    
    def test_integer_addition(self):
        query = mldb.query('select * from test where latest_timestamp(imp) < latest_timestamp(click) order by rowName()')

        self.assertTableResultEquals(query,
            [
                ["_rowName", "click", "imp"],
                ["imp_then_click_1d", 0, 0],
                ["imp_then_click_1month", 0, 0] 
            ]
        )

        query = mldb.query('select * from test where latest_timestamp(imp) > latest_timestamp(click)')

        self.assertTableResultEquals(query,
            [
                ["_rowName", "click", "imp"],
                ["click_then_imp_2d", 0, 0]
            ]
        )

        # integers are interpreted as # of days
        query = mldb.query('select * from test where latest_timestamp(imp)  < latest_timestamp(click) + 3 order by rowName()')

        self.assertTableResultEquals(query,
            [
                ["_rowName", "click", "imp"],
                ["click_then_imp_2d", 0, 0],
                ["imp_then_click_1d", 0, 0],
                ["imp_then_click_1month", 0, 0]
            ]
        )

        # commutativity
        query = mldb.query('select * from test where latest_timestamp(imp) < 3 + latest_timestamp(click) order by rowName()')

        self.assertTableResultEquals(query,
            [
                ["_rowName", "click", "imp"],
                ["click_then_imp_2d", 0, 0],
                ["imp_then_click_1d", 0, 0],
                ["imp_then_click_1month", 0, 0]
            ]
        )


    def test_interval_addition(self):
        query = mldb.query("select * from test where latest_timestamp(imp) + INTERVAL '1d' = latest_timestamp(click)")

        self.assertTableResultEquals(query,
            [
                ["_rowName", "click", "imp"],
                ["imp_then_click_1d", 0, 0] 
            ]
        )

        query = mldb.query("select * from test where INTERVAL '1d' + latest_timestamp(imp) = latest_timestamp(click)")

        self.assertTableResultEquals(query,
            [
                ["_rowName", "click", "imp"],
                ["imp_then_click_1d", 0, 0] 
            ]
        )

        query = mldb.query("select * from test where latest_timestamp(imp) + INTERVAL '1month' = latest_timestamp(click)")

        self.assertTableResultEquals(query,
            [
                ["_rowName", "click", "imp"],
                ["imp_then_click_1month", 0, 0] 
            ]
        )

        query = mldb.query("select * from test where latest_timestamp(imp) + INTERVAL '29day 33m' > latest_timestamp(click)")

        self.assertTableResultEquals(query,
            [
                ["_rowName", "click", "imp"],
                ["imp_then_click_1d", 0, 0],
                ["click_then_imp_2d", 0, 0]
            ]
        )

        query = mldb.query("select * from test where latest_timestamp(click) - latest_timestamp(imp) <= interval '1month' order by rowName()")

        self.assertTableResultEquals(query,
            [
                ["_rowName", "click", "imp"],
                ["click_then_imp_2d", 0, 0],
                ["imp_then_click_1d", 0, 0],
                ["imp_then_click_1month", 0, 0]
            ]
        )

        query = mldb.query("select * from test where latest_timestamp(click) - 1 >= latest_timestamp(imp) order by rowName()")

        self.assertTableResultEquals(query,
            [
                ["_rowName", "click", "imp"],
                ["imp_then_click_1d", 0, 0],
                ["imp_then_click_1month", 0, 0]
            ]
        )

        query = mldb.query("select * from test where latest_timestamp(click) - interval '1d' >= latest_timestamp(imp) order by rowName()")

        self.assertTableResultEquals(query,
            [
                ["_rowName", "click", "imp"],
                ["imp_then_click_1d", 0, 0],
                ["imp_then_click_1month", 0, 0]
            ]
        )

    def test_interval_comparison(self):
        self.assertTableResultEquals(mldb.query("select interval '1d' + interval '2d' < interval '20d' as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select interval '1d' > - interval '1d' as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select - interval '1d' > - - interval '1d' as result"),
             [
                ["_rowName", "result"],
                [       "",  False ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select interval '3d' - interval '1d' < interval '3d' as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select interval '1d' * 3 = interval '3d' as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select 3 * interval '1d' >=interval '2d' as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select 0.5 * interval '4d' = interval '2d' as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select interval '4d' / 2 = interval '2d' as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )


    def test_MLDB_903(self):
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'test3',
        }

        dataset3 = mldb.create_dataset(dataset_config)
        dataset3.record_row('myrow', [ ["a", 0, self.ts], ["b", 0, self.ts_plus_1d], ["c", 0, self.ts_plus_2d] ])
        
        dataset3.commit()

        query = mldb.get('/v1/datasets/test3/query', select = 'latest_timestamp({a,b}) as latest, earliest_timestamp({a,b}) as earliest')

        self.assertFullResultEquals(query.json(),
             [
                 {
                     "rowName": "myrow",
                     "rowHash": "fbdba4c9be68f633",
                     "columns": [
                         [
                             "latest",
                             {
                                 "ts": "2015-01-02T00:00:00Z"
                             },
                             "2015-01-02T00:00:00Z"
                         ],
                         [
                             "earliest",
                             {
                                 "ts": "2015-01-01T00:00:00Z"
                             },
                             "2015-01-02T00:00:00Z"
                         ]
                     ]
                 }
             ]
        )
        
        query = mldb.get('/v1/datasets/test3/query',  select = 'latest_timestamp({*}) as latest, earliest_timestamp({*}) as earliest')

        self.assertFullResultEquals(query.json(),
             [
                 {
                     "rowName": "myrow",
                     "rowHash": "fbdba4c9be68f633",
                     "columns": [
                         [
                             "latest",
                             {
                                 "ts": "2015-01-03T00:00:00Z"
                             },
                             "2015-01-03T00:00:00Z"
                         ],
                         [
                             "earliest",
                             {
                                 "ts": "2015-01-01T00:00:00Z"
                             },
                             "2015-01-03T00:00:00Z"
                         ]
                     ]
                 }
             ]
        )

        query = mldb.get('/v1/datasets/test3/query', select = 'latest_timestamp({a, {b, c}}) as latest, earliest_timestamp({a, {b, c}})as earliest')

        self.assertFullResultEquals(query.json(),
            [
                 {
                     "rowName": "myrow",
                     "rowHash": "fbdba4c9be68f633",
                     "columns": [
                         [
                             "latest",
                             {
                                 "ts": "2015-01-03T00:00:00Z"
                             },
                             "2015-01-03T00:00:00Z"
                         ],
                         [
                             "earliest",
                             {
                                 "ts": "2015-01-01T00:00:00Z"
                             },
                             "2015-01-03T00:00:00Z"
                         ]
                     ]
                 }
             ]
        )


    def test_MLDB_1370(self):
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'test4',
        }

        dataset4 = mldb.create_dataset(dataset_config)
        dataset4.record_row('myrow', [ [ "a", 0, self.ts ], ["a", 0, self.ts_plus_1d] ])
        dataset4.commit()

        query1 = mldb.get('/v1/datasets/test4/query', select = 'earliest_timestamp(a) as earliest')
        query2 = mldb.get('/v1/datasets/test4/query', select = 'earliest_timestamp({*}) as earliest')
        
        self.assertFullResultEquals(query1.json(), query2.json())


    def test_MLDB_1357(self):
        # one minute after epoch
        self.assertTableResultEquals(mldb.query("select timestamp 60 = timestamp '1970-01-01T00:01:00' as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select timestamp 61 = timestamp '1970-01-01T00:01:00' as result"),
             [
                ["_rowName", "result"],
                [       "",  False ]
            ]
        )

        # idempotence
        self.assertTableResultEquals(mldb.query("select timestamp timestamp 123 = timestamp 123 as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select 1 @ timestamp 123 = 1 @ '1970-01-01T00:02:03' as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select 2 @ timestamp 123 != 1 @ '1970-01-01T00:02:03' as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select latest_timestamp(2 @ timestamp 123) = latest_timestamp(1 @ '1970-01-01T00:02:03') as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select latest_timestamp(1) = TIMESTAMP -Inf as result"),
             [
                ["_rowName", "result"],
                [       "",  True ]
            ]
        )

        self.assertTableResultEquals(mldb.query("select latest_timestamp(1) = TIMESTAMP Inf as result"),
             [
                ["_rowName", "result"],
                [       "",  False ]
            ]
        )
    def test_MLDB_1453(self):
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'test5',
        }

        dataset = mldb.create_dataset(dataset_config)
        dataset.record_row('myrow', [ [ "a", 0, self.ts ] ])
        dataset.commit()

        query1 = mldb.get('/v1/datasets/test5/query', select = 'a IS NOT TIMESTAMP as x, a IS TIMESTAMP as y')

        self.assertFullResultEquals(query1.json(),
            [{"rowName":"myrow","rowHash":"fbdba4c9be68f633","columns":[["x",1,"2015-01-01T00:00:00Z"],["y",0,"2015-01-01T00:00:00Z"]]}]
        )

        query1 = mldb.get('/v1/datasets/test5/query', select = 'latest_timestamp(1) IS NOT TIMESTAMP as x, latest_timestamp(1) IS TIMESTAMP as y')

        self.assertFullResultEquals(query1.json(),
            [{"rowName":"myrow","rowHash":"fbdba4c9be68f633","columns":[["x",0,"-Inf"],["y",1,"-Inf"]]}]
        )    

        query1 = mldb.get('/v1/datasets/test5/query', select = "interval '3d' IS NOT INTERVAL as x, interval '3d' IS INTERVAL as x")

        self.assertFullResultEquals(query1.json(),
            [{"rowName": "myrow","rowHash": "fbdba4c9be68f633","columns":[["x",0,"-Inf"],["x",1,"-Inf"]]}]
        )    

mldb.run_tests()

