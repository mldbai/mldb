#
# MLDB-2112_500_on_broken_proc_test.py
# Francois-Michel L'Heureux, 2017-01-12
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2112500OnBrokenProcTest(MldbUnitTest):  # noqa

    def test_it(self):
        with self.assertRaises(mldb_wrapper.ResponseException) as e:
            mldb.put('/v1/procedures/proc', {
                'type' : 'transform',
                'params' : {
                    'inputData' : 'SELECT x:BUG:1',
                    'outputDataset' : 'perruche'
                }
            })

        # this works
        mldb.log(mldb.get('/v1/procedures/proc'))

        with self.assertRaises(mldb_wrapper.ResponseException) as e:
            mldb.get('/v1/procedures/proc/runs')

        self.assertEqual(e.exception.response.status_code, 404)


if __name__ == '__main__':
    mldb.run_tests()
