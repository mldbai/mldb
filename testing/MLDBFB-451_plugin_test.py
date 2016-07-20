#
# MLDBFB-451_plugin_test.py
# Francois-Michel L Heureux, 2016-07-20
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldbfb451PluginTest(MldbUnitTest):  # noqa

    def test_it(self):
        # this call used to fail with 400 if mldb_wrapper was used in main.py
        mldb.delete('/v1/plugins/mldbfb541')
        mldb.put('/v1/plugins/mldbfb541', {
            'type' : 'python',
            'params' : {
                'address' : 'file://mldb/testing/MLDBFB-451-plugin'
            }
        })

if __name__ == '__main__':
    mldb.run_tests()
