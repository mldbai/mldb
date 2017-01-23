# MLDB-1802-join-order-by.py
# Mathieu Marquis Bolduc, 2016-07-12
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest
import json

mldb = mldb_wrapper.wrap(mldb) # noqa

class DatasetFunctionTest(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        
        ds = mldb.create_dataset({ "id": "dataset1", "type": "sparse.mutable" })
        ds.record_row("row_c",[["x", 1, 0], ["y", 3, 0]])
        ds.record_row("row_b",[["x", 2, 0], ["y", 2, 0]])
        ds.record_row("row_a",[["x", 3, 0], ["y", 1, 0]])
        ds.commit()
        
    def test_join_order_by(self):

        query = """
                SELECT %s
                FROM dataset1
                ORDER BY dataset1.x, x.rowHash()
                """

        res1 = mldb.query(query % '1')
        res2 = mldb.query(query % 'dataset1.y')

        #original issue was that res2 had the rows in a different (wrong) order than res1

        expected1 = [["_rowName","1"],
                    ["row_c", 1],
                    ["row_b", 1],
                    ["row_a", 1]]

        expected2 = [["_rowName","dataset1.y"],
                    ["row_c", 3],
                    ["row_b", 2],
                    ["row_a", 1]]

        self.assertTableResultEquals(res1, expected1)
        self.assertTableResultEquals(res2, expected2)

mldb.run_tests()
