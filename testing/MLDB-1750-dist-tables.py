#
# MLDB-1750-dist-tables.py
# Simon Lemieux, 2016-06-27
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

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
        self.assertTableResultEquals(
            mldb.query('select * from bid_req_features order by rowName()'),
            [
                ['_rowName',
                 'price.host.count', 'price.host.avg',
                 'price.region.count', 'price.region.avg',
                 'target.host.count', 'target.host.avg',
                 'target.region.count', 'target.region.avg'],
                ['row0'] + [None for x in range(8)],
                ['row1', None, None, 1, 1, None, None, 1, 2],
                ['row2', 1, 3, None, None, 1, 4, None, None],
                ['row3', 1, 1, None, None, 1, 2, None, None],
                ['row4', 2, 5, 1, 9, 2, 6, None, None],
            ]
        )

        # create a function and call it (one was already created but let's test
        # this anyway
        mldb.put('/v1/functions/get_stats2', {
            'type': 'distTable.getStats',
            'params': {
                'distTableFileUrl': 'file://dt.dt'
            }
        })

        for fname in ['get_stats', 'get_stats2']:
            self.assertTableResultEquals(
                mldb.query("""
                        SELECT %s({keys: {host: 'patate.com', region: 'usa'}}) AS *,
                        """ % fname),
                [
                    ['_rowName',
                    'stats.price.host.count', 'stats.price.host.avg',
                    'stats.price.region.count', 'stats.price.region.avg',
                    'stats.target.host.count', 'stats.target.host.avg',
                    'stats.target.region.count', 'stats.target.region.avg',
                     ],
                    ['result', 2, 5, 2, 10,
                               1, 2, 1, 10]
                ]
            )


if __name__ == '__main__':
    mldb.run_tests()
