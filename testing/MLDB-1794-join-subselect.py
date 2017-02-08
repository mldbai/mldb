# MLDB-1794-join-subselect.py
# Mathieu Marquis Bolduc, 2016-12-07
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
        ds.record_row("row1",[["x", "toy story", 0],["y", "1", 0]])
        ds.record_row("row2",[["x", "terminator", 0],["y", "2", 0]])
        ds.commit()

    def test_join_subselect_groupby(self):        

        res = mldb.query("select a.x from dataset1 as a INNER JOIN ( SELECT x from dataset1 GROUP BY x) as b ON a.x = b.x AND a.y != b.x")
        mldb.log(res)

        expected = [["_rowName","a.x"],
                    ["\"[row2]-[\"\"[\"\"\"\"terminator\"\"\"\"]\"\"]\"","terminator"],
                    ["\"[row1]-[\"\"[\"\"\"\"toy story\"\"\"\"]\"\"]\"","toy story"]]

        self.assertEqual(res, expected)    

mldb.run_tests()
