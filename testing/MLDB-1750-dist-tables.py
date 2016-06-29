#
# MLDB-1750-dist-tables.py
# Simon Lemieux, 2016-06-27
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

from math import sqrt

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1750DistTables(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        headers = ['host', 'region', 'price', 'target2', 'order_']
        data = [
            ('patate.com', 'canada', 1,  2,    0),
            ('poil.com',   'canada', 3,  4,    1),
            ('poil.com',   None,     7,  8,    2),
            ('patate.com', 'usa',    9,  None, 3),
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

    def test_it(self):
        # call the distTable.train procedure
        mldb.post('/v1/procedures', {
            'type': 'distTable.train',
            'params': {
                'trainingData': """ SELECT host, region
                                    FROM bid_req
                                    ORDER BY order_
                                """,
                'outputDataset': 'bid_req_features',
                'outcomes': [['price', 'price'], ['target', 'target2']],
                'distTableFileUrl': 'file://dt.dt',
                'functionName': 'get_stats',
                'runOnCreation': True
            }
        })

        # check that the running stats (the features for a bid request) are as
        # expected
        stats = ['count', 'avg', 'std', 'min', 'max']
        stats_no_std = ['count', 'avg', 'min', 'max']
        expected = [
            ['_rowName']
            + ['price.host.' + s for s in stats]
            # price.region.std is always NULL in my exemple, and so the
            # column won't be returned
            + ['price.region.' + s for s in stats_no_std]
            + ['target.host.' + s for s in stats]
            + ['target.region.' + s for s in stats_no_std],

            ['row0'] + [0, None, None, None, None,
                        0, None, None, None,
                        0, None, None, None, None,
                        0, None, None, None],
            ['row1',
                # price for host = poil.com
                0, None, None, None, None,
                # price for region = canada
                1, 1, 1, 1,
                # target for host = poil.com
                0, None, None, None, None,
                # target for region = canada
                1, 2, 2, 2],
            ['row2',
                # price for host = poil.com
                1, 3, None, 3, 3,
                # price for region = None
                0, None, None, None,
                # target for host = poil.com
                1, 4, None, 4, 4,
                # target for region = None
                0, None, None, None],
            ['row3',
                # price for host = patate.com
                1, 1, None, 1, 1,
                # price for region  = usa
                0, None, None, None,
                # target for host = patate.com
                1, 2, None, 2, 2,
                # target for region = usa
                0, None, None, None],
            ['row4',
                # price for host = poil.com
                2, 5, 2 * sqrt(2.), 3, 7,
                # price for region = usa
                1, 9, 9, 9,
                # target for host = poil.com
                2, 6, 2 * sqrt(2.), 4, 8,
                # target for region = usa
                0, None, None, None],
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
            'type': 'distTable.getStats',
            'params': {
                'distTableFileUrl': 'file://dt.dt'
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
                    + ['stats.target.host.' + s for s in stats_no_std]
                    + ['stats.target.region.' + s for s in stats_no_std],
                    ['result',
                     # data = [1,9]
                     2, 5, sqrt(32.), 1, 9,
                     # data = [9,11]
                     2, 10, sqrt(2.), 9, 11,
                     # data = [2]
                     1, 2, 2, 2,
                     # data = [10]
                     1, 10, 10, 10]
                ]
            )

        # unknown columns
        self.assertTableResultEquals(
            mldb.query("""
                SELECT %s({features: {host: 'prout', region: 'prout'}}) AS *
            """ % fname),
            [
                ['_rowName', 'stats.price.host.count',
                    'stats.price.region.count', 'stats.target.host.count',
                    'stats.target.region.count'],
                ['result', 0, 0, 0, 0]
            ]
        )


if __name__ == '__main__':
    mldb.run_tests()
