#
# MLDB-1661-function-name-conflict.py
# 2016-05-31
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class FunctionNameTest(MldbUnitTest):  

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "dataset", "type": "sparse.mutable" })
        ds.record_row("a",[["x", "toy story", 0]])
        ds.commit()     


    def test_valid_group_by(self):

        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            res = mldb.put('/v1/functions/temporal_earliest', {
                    'type': 'sql.query',
                    'params': {
                  'query': 'SELECT temporal_earliest({}) FROM dataset'
                    }})

        result = re.exception.response
        mldb.log(result)

        self.assertTrue("MLDB already has a built-in function named" in result.json()["error"], "did not get the expected message MLDB-1661")

        res = mldb.query('SELECT temporal_earliest({}) FROM dataset')

mldb.run_tests()
