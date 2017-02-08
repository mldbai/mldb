# MLDBFB-636-join-rowhash.py
# Mathieu Marquis Bolduc, 2016-07-15
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest
import json

mldb = mldb_wrapper.wrap(mldb) # noqa

class DatasetFunctionTest(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "dataset1", "type": "sparse.mutable" })
        ds.record_row("row1",[["x", "1", 0]])
        ds.record_row("row3",[["x", "3", 0]])
        ds.record_row("row2",[["x", "2", 0]])
        ds.commit()

    def test_join_rowhash(self):        

        res = mldb.query("SELECT x.rowHash(), x.rowName() FROM dataset1 as x JOIN dataset1 as y ON x.rowHash() = y.rowHash() ORDER BY x.rowHash()")
        mldb.log(res)

        expected = [["_rowName","\"x.rowHash()\"","\"x.rowName()\""],
                    ["[row3]-[row3]", 5165065399359991982, "row3"],
                    ["[row1]-[row1]", 9657511930026400460, "row1"],
                    ["[row2]-[row2]", 11939232387177687614, "row2"]]

        self.assertEqual(res, expected)    

mldb.run_tests()
