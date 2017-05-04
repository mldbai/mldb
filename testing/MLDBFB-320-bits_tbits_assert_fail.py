#
# MLDBFB-320-bits_tbits_assert_fail.py
# Mich, 2016-01-21
# This file is part of MLDB. Copyright 20165 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class NonFiniteDateTest(MldbUnitTest):  

    @classmethod
    def setUpClass(self):
        ds = mldb.create_dataset({ "id": "ds", "type": "sparse.mutable" })
        ds.record_row("row1",[['uid', 'user1', 0], ['ts', 0, 0]])
        ds.commit()

    # NOTE: non-finite timestamps are now mapped automatically to Date()
    # this provides a better user experience, at the expense of some
    # possible errors.
    # def test_beh_error_on_inf(self):
    #     with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
    #                               'Cannot record non-finite timestamp into behaviour dataset'):
    #         res = mldb.post( '/v1/procedures', {
    #             'type': 'transform',
    #             'params' : {
    #                 'inputData' : 'SELECT ds.uid, ds.ts, 1 AS weight FROM ds',
    #                 'outputDataset' : {
    #                     'id' : 'resDs',
    #                     'type' : 'beh.mutable'
    #                 },
    #                 'runOnCreation' : True
    #             }
    #         })
    #         mldb.log(res)


    def test_beh_success_on_inf(self):
        mldb.post( '/v1/procedures', {
            'type': 'transform',
            'params' : {
                'inputData' : 'SELECT ds.uid, ds.ts, 1 @ 0  AS weight FROM ds',
                'outputDataset' : {
                'id' : 'resDs',
                    'type' : 'beh.mutable'
                },
                'runOnCreation' : True
            }
        })


mldb.run_tests()
   

