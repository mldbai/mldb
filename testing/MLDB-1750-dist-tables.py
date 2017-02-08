#
# MLDB-1750-dist-tables.py
# Simon Lemieux, 2016-06-27
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import os, tempfile
from math import sqrt

mldb = mldb_wrapper.wrap(mldb)  # noqa

def get_temporary_file():
    return tempfile.NamedTemporaryFile(prefix=os.getcwd() + '/build/x86_64/tmp')

NaN = 'NaN'

class MLDB1750DistTables(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        headers = ['host', 'region', 'price', 'target2', 'order_']
        data = [
            ('patate.com', 'canada', 1,  2,    0),
            ('poil.com',   'canada', 3,  4,    1),
            ('poil.com',   None,     7,  8,    2),
            ('patate.com', 'usa',    9,  10,   3),
            ('poil.com',   'usa',    11, 10,   4),
        ]

        mldb.put('/v1/datasets/bid_req', {
            'type': 'sparse.mutable'
        })

        for i,row in enumerate(data):
            mldb.post('/v1/datasets/bid_req/rows', {
                'rowName': 'row' + str(i),
                'columns': [[k,v,0] for k,v in zip(headers, row)]
            })
        mldb.post('/v1/datasets/bid_req/commit')

        # data for bag of words test
        headers = ['tag_a', 'tag_b', 'tag_c', 'price', 'target2', 'order_']
        data = [
            (1,    1,    None, 1,  2,    0),
            (None, None, 1,    3,  4,    1),
            (None, None, None, 7,  8,    2),
            (None, 1,    1,    9,  10,   3),
            (1,    1,    1,    11, 10,   4),
        ]

        mldb.put('/v1/datasets/tags', {
            'type': 'sparse.mutable'
        })

        for i,row in enumerate(data):
            mldb.post('/v1/datasets/tags/rows', {
                'rowName': 'row' + str(i),
                'columns': [[k,v,0] for k,v in zip(headers, row) if v is not None]
            })
        mldb.post('/v1/datasets/tags/commit')

    def test_it(self):
        _dt_file = get_temporary_file()
        dt_file = 'file:///' + _dt_file.name

        # call the distTable.train procedure
        mldb.post('/v1/procedures', {
            'type': 'experimental.distTable.train',
            'params': {
                'trainingData': """ SELECT host, region
                                    FROM bid_req
                                    ORDER BY order_
                                """,
                'outputDataset': 'bid_req_features',
                'outcomes': [['price', 'price'], ['target', 'target2']],
                'distTableFileUrl': dt_file,
                'functionName': 'get_stats',
                'runOnCreation': True
            }
        })

        # check that the running stats (the features for a bid request) are as
        # expected
        stats = ['count', 'avg', 'std', 'min', 'max']
        expected = [
            ['_rowName']
            + ['price.host.' + s for s in stats]
            # price.region.std is always NULL in my example, and so the
            # column won't be returned
            + ['price.region.' + s for s in stats]
            + ['target.host.' + s for s in stats]
            + ['target.region.' + s for s in stats],

            ['row0'] + [0, NaN, NaN, NaN, NaN,
                        0, NaN, NaN, NaN, NaN,
                        0, NaN, NaN, NaN, NaN,
                        0, NaN, NaN, NaN, NaN],
            ['row1',
                # price for host = poil.com
                0, NaN, NaN, NaN, NaN,
                # price for region = canada
                1, 1, NaN, 1, 1,
                # target for host = poil.com
                0, NaN, NaN, NaN, NaN,
                # target for region = canada
                1, 2, NaN, 2, 2],
            ['row2',
                # price for host = poil.com
                1, 3, NaN, 3, 3,
                # price for region = NaN
                0, NaN, NaN, NaN, NaN,
                # target for host = poil.com
                1, 4, NaN, 4, 4,
                # target for region = NaN
                0, NaN, NaN, NaN, NaN],
            ['row3',
                # price for host = patate.com
                1, 1, NaN, 1, 1,
                # price for region  = usa
                0, NaN, NaN, NaN, NaN,
                # target for host = patate.com
                1, 2, NaN, 2, 2,
                # target for region = usa
                0, NaN, NaN, NaN, NaN],
            ['row4',
                # price for host = poil.com
                2, 5, 2 * sqrt(2.), 3, 7,
                # price for region = usa
                1, 9, NaN, 9, 9,
                # target for host = poil.com
                2, 6, 2 * sqrt(2.), 4, 8,
                # target for region = usa
                1, 10, NaN, 10, 10],
        ]

        # mldb.log(mldb.query('select * from bid_req_features order by rowName()'))
        # mldb.log(expected)

        self.assertTableResultEquals(
            mldb.query('select * from bid_req_features order by rowName()'),
            expected
        )

        # create a function and call it (one was already created but let's test
        # this anyway)
        mldb.put('/v1/functions/get_stats2', {
            'type': 'experimental.distTable.getStats',
            'params': {
                'distTableFileUrl': dt_file
            }
        })

        for fname in ['get_stats', 'get_stats2']:
            self.assertTableResultEquals(
                mldb.query("""
                    SELECT %s({features: {host: 'patate.com', region: 'usa'}}) AS *
                """ % fname),
                [
                    ['_rowName']
                    + ['stats.price.host.' + s for s in stats]
                    + ['stats.price.region.' + s for s in stats]
                    + ['stats.target.host.' + s for s in stats]
                    + ['stats.target.region.' + s for s in stats],
                    ['result',
                     # data = [1,9]
                     2, 5, sqrt(32.), 1, 9,
                     # data = [9,11]
                     2, 10, sqrt(2.), 9, 11,
                     # data = [2, 10]
                     2, 6, sqrt(32.), 2, 10,
                     # data = [10]
                     2, 10, 0, 10, 10]
                ]
            )

        # unknown columns
        self.assertTableResultEquals(
            mldb.query("""
                SELECT %s({features: {host: 'prout', region: 'prout'}}) AS *
            """ % fname),
            [
                ['_rowName']
                + ['stats.price.host.' + s for s in stats]
                + ['stats.price.region.' + s for s in stats]
                + ['stats.target.host.' + s for s in stats]
                + ['stats.target.region.' + s for s in stats],
                ['result',
                 0, NaN, NaN, NaN, NaN,
                 0, NaN, NaN, NaN, NaN,
                 0, NaN, NaN, NaN, NaN,
                 0, NaN, NaN, NaN, NaN]
            ]
        )

        # unpack the stats
        mldb.query("""
            SELECT get_stats({features: {host: 'patate.com'}})[stats] AS *
        """)

    def test_unknown_stats(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                'Unknown distribution table statistic'):
            mldb.post('/v1/procedures', {
                'type': 'experimental.distTable.train',
                'params': {
                    'trainingData': """ SELECT host, region
                                        FROM bid_req
                                        ORDER BY order_
                                    """,
                    'outputDataset': 'bid_req_features_few_stats',
                    'outcomes': [['price', 'price']],
                    'distTableFileUrl': "file://tmp/mldb-1750_non_default_stats.dt",
                    'functionName': 'get_stats',
                    'statistics': ['patate'],
                    'runOnCreation': True
                }
            })

    def test_non_default_stats(self):
        # call the distTable.train procedure
        mldb.post('/v1/procedures', {
            'type': 'experimental.distTable.train',
            'params': {
                'trainingData': """ SELECT host, region
                                    FROM bid_req
                                    ORDER BY order_
                                """,
                'outputDataset': 'bid_req_features_few_stats',
                'outcomes': [['price', 'price']],
                'distTableFileUrl': "file://tmp/mldb-1750_non_default_stats.dt",
                'functionName': 'get_stats',
                'statistics': ['last', 'min', 'sum'],
                'runOnCreation': True
            }
        })


        self.assertTableResultEquals(
            mldb.query("select * from bid_req_features_few_stats where rowName() = 'row4'"),
            [["_rowName", "price.host.last", "price.host.min", "price.host.sum",
                          "price.region.last", "price.region.min", "price.region.sum"],
             ["row4", 7, 3, 10, # values: [3,7]
                      9, 9, 9]  # values: [9]
            ])

        self.assertTableResultEquals(
            mldb.query("""
                select get_stats({features: {host, region}})[stats] as *
                from bid_req
                where rowName() = 'row4'
            """),
            [["_rowName", "price.host.last", "price.host.min", "price.host.sum",
                          "price.region.last", "price.region.min", "price.region.sum"],
             ["row4", 11, 3, 21, # values: [3,7,11]
                      11, 9, 20] # values: [9,11]
            ])

        # create a function  with different stats
        mldb.put('/v1/functions/get_stats_non_default', {
            'type': 'experimental.distTable.getStats',
            'params': {
                'distTableFileUrl': "file://tmp/mldb-1750_non_default_stats.dt",
                'statistics': ['max']
            }
        })

        self.assertTableResultEquals(
            mldb.query("""
                SELECT get_stats_non_default({features: {host: 'prout', region: 'usa'}}) AS *
            """),
            [
                [
                    "_rowName",
                    "stats.price.host.max",
                    "stats.price.region.max"
                ],
                [
                    "result",
                    "NaN",
                    11
                ]
            ])

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                'Unknown distribution table statistic'):
            mldb.put('/v1/functions/get_stats_non_default2', {
                'type': 'experimental.distTable.getStats',
                'params': {
                    'distTableFileUrl': "file://tmp/mldb-1750_non_default_stats.dt",
                    'statistics': ['pwel']
                }
            })



    def test_bow_dist_tables(self):
        _dt_file = get_temporary_file()
        dt_file = 'file:///' + _dt_file.name

        # call the distTable.train procedure
        mldb.post('/v1/procedures', {
            'type': 'experimental.distTable.train',
            'params': {
                'trainingData': """ SELECT tag*
                                    FROM tags
                                    ORDER BY order_
                                """,
                'outcomes': [['price', 'price']],
                'distTableFileUrl': dt_file,
                'mode': 'bagOfWords',
                'statistics': ["avg", "max"],
                'functionName': 'get_bow_stats',
                'runOnCreation': True
            }
        })

        self.assertTableResultEquals(
            mldb.query("""
                SELECT get_bow_stats({features: {"tag_a": 1, "tag_b":1, "tag_c":1}})[stats] AS *
            """),
            [
                [
                    "_rowName",
                    "price.tag_a.avg",
                    "price.tag_a.max",
                    "price.tag_b.avg",
                    "price.tag_b.max",
                    "price.tag_c.avg",
                    "price.tag_c.max"
                ],
                [
                    "result",
                    6,
                    11,
                    7,
                    11,
                    7.666666666666667,
                    11
                ]
            ])

        self.assertTableResultEquals(
            mldb.query("""
            SELECT get_bow_stats({features: {"tag_z": 1}})[stats] AS *
            """),
            [["_rowName", "price.tag_z.avg", "price.tag_z.max"],
             ["result", 'NaN', 'NaN']])

    def test_null_col(self):
        """ when a column is NULL, it is treated as if NULL was a value
        """
        headers = ['host', 'patate', 'price', 'order']
        data = [
            ('patate.com', None, 1, 1),
            ('poil.com',   None, 3, 2),
            ('patate.com', None, 8, 3),
        ]

        mldb.put('/v1/datasets/poil', {
            'type': 'sparse.mutable'
        })

        for i,row in enumerate(data):
            mldb.post('/v1/datasets/poil/rows', {
                'rowName': 'row' + str(i),
                'columns': [[k,v,0] for k,v in zip(headers, row)]
            })
        mldb.post('/v1/datasets/poil/commit')

        _dt_file = get_temporary_file()
        dt_file = 'file:///' + _dt_file.name

        mldb.post('/v1/procedures', {
            'type': 'experimental.distTable.train',
            'params': {
                'trainingData': """ SELECT * EXCLUDING(price, order)
                                    FROM poil
                                    ORDER BY order
                                """,
                'outcomes': [['price', 'price']],
                'distTableFileUrl': dt_file,
                'statistics': ["avg"],
                'functionName': 'get_bow_stats',
                'runOnCreation': True,
                'outputDataset': 'poil_dt',
            }
        })

        res = mldb.query("""
            SELECT price.host.avg, price.patate.avg
            FROM merge(poil_dt, poil)
            ORDER BY order
        """)
        truth = [
            ['_rowName', 'price.host.avg', 'price.patate.avg'],
            ['row0', NaN, NaN],
            ['row1', NaN, 1],
            ['row2', 1, 2],
        ]
        self.assertTableResultEquals(res, truth)

        res = mldb.query("""
            SELECT get_bow_stats({features: {host:'patate.com', patate:NULL}})[stats] as *
        """)
        truth = [
            ['_rowName', 'price.host.avg', 'price.patate.avg'],
            ['result', 4.5, 4],
        ]
        self.assertTableResultEquals(res, truth)

        res = mldb.query("""
            SELECT get_bow_stats({features: {host:'poil.com', patate:NULL}})[stats] as *
        """)
        truth = [
            ['_rowName', 'price.host.avg', 'price.patate.avg'],
            ['result', 3, 4],
        ]
        self.assertTableResultEquals(res, truth)


    def test_real_time(self):
        # call the distTable.train procedure
        mldb.post('/v1/procedures', {
            'type': 'experimental.distTable.train',
            'params': {
                'trainingData': """ SELECT host, region
                                    FROM bid_req
                                    ORDER BY order_
                                """,
                'outcomes': [['price', 'price']],
                'distTableFileUrl': "file://tmp/mldb-1750_non_default_stats_rt.dt",
                'functionName': 'get_stats_rt',
                'statistics': ['last', 'min', 'sum'],
                'runOnCreation': True
            }
        })

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                'Unknown dist table \'hosti234234\''):
            mldb.post("/v1/functions/get_stats/routes/increment", {
                'keys': [
                    ['hosti234234', 'pwet']
                ],
                'outcomes': [50]
            })

        mldb.log(mldb.query("""
            SELECT get_stats({features: {host: 'pwet'}})[stats] AS *
        """))

        def incrementAndTest(keys, outcomes, expected, fnct="get_stats"):
            if outcomes:
                mldb.log(mldb.post("/v1/functions/get_stats/routes/increment", {
                        'keys': keys,
                        'outcomes': outcomes
                    }))

            self.assertTableResultEquals(
                mldb.query("SELECT %s({features: {host: '%s'}})[stats] AS *" % (fnct, keys[0][1])),
                [["_rowName", "price.host.last", "price.host.min", "price.host.sum"],
                 ["result"] + expected])

        incrementAndTest([['host', 'patate']], None, ["NaN", "NaN", 0])
        incrementAndTest([['host', 'patate']], [50], [50, 50, 50])
        incrementAndTest([['host', 'patate']], [150], [150, 50, 200])

        # dump to disk with the updated counts
        mldb.post("/v1/functions/get_stats/routes/persist", {
                'modelFileUrl': "file://tmp/mldb-1750_stats_rt_persist.dt"
            })

        # reload in new function
        mldb.put('/v1/functions/get_stats_reloaded', {
            'type': 'experimental.distTable.getStats',
            'params': {
                'distTableFileUrl': "file://tmp/mldb-1750_stats_rt_persist.dt",
                'statistics': ['last', 'min', 'sum']
            }
        })
        # make sure the counts are the updated counts
        incrementAndTest([['host', 'patate']], None, [150, 50, 200], "get_stats_reloaded")



if __name__ == '__main__':
    mldb.run_tests()
