#
# ranking_procedure_test.py
# Mich, 2016-01-11
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class RankingTest(MldbUnitTest):  # noqa
    def test_base(self):
        mldb.put('/v1/datasets/ds', {
            'type' : 'sparse.mutable',
        })

        size = 123
        for i in xrange(size):
            mldb.post('/v1/datasets/ds/rows', {
                'rowName' : 'row{}'.format(i),
                'columns' : [['score', i, 1], ['index', i * 2, 2], ['prob', i * 3, 3]]
            })

        mldb.post('/v1/datasets/ds/commit')

        mldb.post('/v1/procedures', {
            'type' : 'ranking',
            'params' : {
                'inputData' : 'SELECT * FROM ds ORDER BY score',
                'outputDataset' : 'out',
                'rankingType' : 'index',
                'runOnCreation' : True
            }
        })

        # MLDB-1267
        mldb.log(mldb.query("SELECT * FROM out"))
        res = mldb.get("/v1/query", q="SELECT latest_timestamp({*}) FROM out",
                       format='table')
        data = res.json()
        self.assertEqual(data[1][1], '1970-01-01T00:00:01Z')

        mldb.log(data[1])
        mldb.put('/v1/datasets/result', {
            'type' : 'merged',
            'params' : {
                'datasets' : [
                    {'id' : 'ds'},
                    {'id' : 'out'}
                ]
            }
        })

        res = mldb.get('/v1/query',
                       q='SELECT score, rank FROM result ORDER BY rank',
                       format='table')
        data = res.json()
        self.assertEqual(data[1][1], 0, str(data[1]))
        self.assertEqual(data[1][2], 0, str(data[1]))
        self.assertEqual(data[2][1], 1, str(data[2]))
        self.assertEqual(data[2][2], 1, str(data[2]))
        self.assertEqual(data[size][1], size - 1, str(data[size]))
        self.assertEqual(data[size][2], size - 1, str(data[size]))

if __name__ == '__main__':
    mldb.run_tests()
