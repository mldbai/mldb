#
# MLDB-1661-function-name-conflict.py
# Mathieu Marquis Bolduc, 2016-05-31
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
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

        with self.assertMldbRaises(expected_regexp="MLDB already has a built-in function named"):
            res = mldb.put('/v1/functions/temporal_earliest', {
                    'type': 'sql.query',
                    'params': {
                  'query': 'SELECT temporal_earliest({}) FROM dataset'
                    }})

        mldb.query('SELECT temporal_earliest({}) FROM dataset')

mldb.run_tests()
