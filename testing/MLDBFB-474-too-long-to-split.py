#
# MLDBFB-474-too-long-to-split.py
# Mich, 2016-04-13
# Copyright (c) 2016 Datacratic Inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class TooLongToSplitTest(MldbUnitTest):  # noqa

    def test_it(self):
        mldb.put('/v1/datasets/ds', {
            'type' : 'beh.binary',
            'params' : {
                'dataFileUrl' : 's3://dev.mldb.datacratic.com/MLDBFB-474-beh.lz4'
            }
        })

        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : 'SELECT * FROM ds ORDER BY rowHash() LIMIT 3000000',
                'outputDataset' : {
                    'type' : 'beh.binary.mutable',
                    'params' : {
                        'dataFileUrl' : 'file://smaller_ds.beh.lz4'
                    }
                },
                'runOnCreation' : True
            }
        })

if __name__ == '__main__':
    mldb.run_tests()
