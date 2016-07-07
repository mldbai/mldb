#
# MLDB-1750-dist-tables.py
# Simon Lemieux, 2016-06-27
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import os, tempfile
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
            ('patate.com', 'usa',    9,  10,    3),
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
        _dt_file = tempfile.NamedTemporaryFile(
            prefix=os.getcwd() + '/build/x86_64/tmp')
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
        NaN = 'NaN'
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



if __name__ == '__main__':
    mldb.run_tests()
