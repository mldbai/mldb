# MLDBFB638-groupby-orderby.py
# Mathieu Marquis Bolduc, 2016-07-14
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
        ds.record_row("row1",[["x", "1", 0], ["y", "1", 0], ["z", "2", 0]])
        ds.record_row("row2",[["x", "2", 0], ["y", "2", 0], ["z", "1", 0]])
        ds.commit()

    def test_join_subselect_groupby(self):        

        res = mldb.query("SELECT min(x) FROM dataset1 GROUP BY y ORDER BY min(z), y")
        mldb.log(res)

        expected = [["_rowName","min(x)"],
                    ["\"[\"\"2\"\"]\"","2"],
                    ["\"[\"\"1\"\"]\"","1"]]

        self.assertEqual(res, expected)    

mldb.run_tests()
