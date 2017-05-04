#
# MLDBFB-505_mldb_query_json_error.py
# Mich, 2016-04-29
# Copyright (c) 2016 mldb.ai inc. All rights reserved.
#
mldb = mldb_wrapper.wrap(mldb)  # noqa

class MldbQueryJsonErrorTest(MldbUnitTest):

    def test_it(self):
        mldb.put('/v1/datasets/ds', {
            'type' : 'beh.binary',
            'params' : {
                'dataFileUrl' : 's3://private-mldb-ai/testing/MLDBFB-505.beh.lz4'
            }
        })

        mldb.query("SELECT * FROM ds")

if __name__ == '__main__':
    mldb.run_tests()
