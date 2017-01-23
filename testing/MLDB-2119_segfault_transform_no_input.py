#
# MLDB-2119_segfault_transform_no_input.py
# Francois-Michel L'Heureux, 2017-01-23
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2119SegfaultTransformNoInput(MldbUnitTest):  # noqa

    def test_run_on_creation(self):
        msg = 'You need to define inputData'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/procedures/run_on_creation', {
                'type' : 'transform',
                'params' : {
                    'skipEmptyRows' : False
                }
            })

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post('/v1/procedures', {
                'type' : 'transform',
                'params' : {
                    'skipEmptyRows' : False
                }
            })

    def test_do_not_run_on_creation(self):
        mldb.put('/v1/procedures/do_not_run_on_creation', {
            'type' : 'transform',
            'params' : {
                'skipEmptyRows' : False,
                'runOnCreation' : False
            }
        })

        msg = 'You need to define inputData'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/procedures/do_not_run_on_creation/runs/r1', {
                'params' : {}
            })

        res = mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'skipEmptyRows' : False,
                'runOnCreation' : False
            }
        }).json()
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post('/v1/procedures/{}/runs'.format(res['id']), {
                'params' : {}
            })



if __name__ == '__main__':
    mldb.run_tests()
