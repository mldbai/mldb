#
# MLDB-1267-bucketize-ts-test.py
# Mich, 2015-11-16
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class BucketizeTest(unittest.TestCase):
    def test_it(self):
        url = '/v1/datasets/input'
        mldb.put(url, {
            'type' : 'sparse.mutable'
        })

        mldb.post(url + '/rows', {
            'rowName' : 'row1',
            'columns' : [['score', 5, 6]]
        })
        mldb.post(url + '/rows', {
            'rowName' : 'row2',
            'columns' : [['score', 1, 5]]
        })

        mldb.post(url + '/commit', {})

        mldb.post('/v1/procedures', {
            'type' : 'bucketize',
            'params' : {
                'inputData' : 'SELECT * FROM input ORDER BY score',
                'outputDataset' : {
                    'id' : 'output',
                    'type' : 'sparse.mutable'
                },
                'percentileBuckets': {'b1': [0, 50], 'b2': [50, 100]},
                'runOnCreation' : True
            }
        })

        res = mldb.query('SELECT latest_timestamp({*}) FROM output')
        self.assertEqual(res[1][1], '1970-01-01T00:00:06Z')

if __name__ == '__main__':
    mldb.run_tests()
