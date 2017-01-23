#
# MLDB-2119_segfault_transform_no_input.py
# Francois-Michel L'Heureux, 2017-01-23
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2119SegfaultTransformNoInput(MldbUnitTest):  # noqa

    def test_it(self):
        msg = 'You need to define inputData'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/procedures/toto', {
                'type' : 'transform',
                'params' : {
                    'skipEmptyRows' : False
                }
            })

if __name__ == '__main__':
    mldb.run_tests()
