# MLDBFB-650-names-aggregators.py
# Mathieu Marquis Bolduc, 2016-07-20
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest
import json

mldb = mldb_wrapper.wrap(mldb) # noqa

class NamedAggregatorTest(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "dataset1", "type": "sparse.mutable" })
        ds.record_row("row1",[["x", "1", 0]])
        ds.record_row("row2",[["x", "1", 0]])
        ds.commit()

    def test_named_in_aggregators(self):        

        res = mldb.query("SELECT x NAMED min(rowName()) FROM dataset1 GROUP BY x")
        mldb.log(res)

        expected = []

        self.assertEqual(res, expected)    

mldb.run_tests()
