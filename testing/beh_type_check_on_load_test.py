#
# beh_type_check_on_load_test.py
# Mich, 2016-06-09
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb)  # noqa

class BehTypeCheckOnLoadTest(MldbUnitTest):  # noqa

    def test_beh(self):
        params = {
            'dataFileUrl' :
                'file://build/x86_64/tmp/BehTypeCheckOnLoadTestBeh.beh'
        }
        ds = mldb.create_dataset({
            'type' : 'beh.mutable',
            'params' : params
        })
        ds.record_row('row1', [['colA', 1, 0]])
        ds.commit()

        # succeeds
        mldb.put('/v1/datasets/beh', {
            'type' : 'beh',
            'params' : params
        })

        msg = "The loaded dataset is not of type beh.binary"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/datasets/error', {
                'type' : 'beh.binary',
                'params' : params
            })

    def test_beh_binary(self):
        params = {
            'dataFileUrl' :
                'file://build/x86_64/tmp/BehTypeCheckOnLoadTestBehBin.beh'
        }
        ds = mldb.create_dataset({
            'type' : 'beh.binary.mutable',
            'params' : params
        })
        ds.record_row('row1', [['colA', 1, 0]])
        ds.commit()

        # succeeds
        mldb.put('/v1/datasets/beh_bin', {
            'type' : 'beh.binary',
            'params' : params
        })

        msg = "The loaded dataset is not of type beh, it is"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/datasets/error', {
                'type' : 'beh',
                'params' : params
            })

if __name__ == '__main__':
    mldb.run_tests()
