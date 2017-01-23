#
# MLDB-1595-count-distinct
# 2016-04-29
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest, json
mldb = mldb_wrapper.wrap(mldb) # noqa

class DistinctTest(unittest.TestCase):

    def test_distinct(self):

        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        ds.record_row("a",[["x", 1, 0]])
        ds.record_row("b",[["x", 2, 0]])
        ds.record_row("c",[])
        ds.record_row("d",[["x", 1, 0]])
        ds.record_row("e",[["x", 3, 0]])
        ds.commit()

        res = mldb.query('''
            select count_distinct(x) as v from sample
        ''')

        mldb.log(res)

        expected = [["_rowName","v"],["[]",3]]
        self.assertEqual(res, expected)

    def test_distinct_row(self):

        ds = mldb.create_dataset({ "id": "sample2", "type": "sparse.mutable" })
        ds.record_row("a",[["x", 1, 0]])
        ds.record_row("b",[["x", 2, 0]])
        ds.record_row("c",[])
        ds.record_row("d",[["x", 1, 0]])
        ds.record_row("e",[["x", 3, 0]])
        ds.commit()

        res = mldb.query('''
            select count_distinct({x as x, x % 2 as y}) as v from sample2
        ''')

        mldb.log(res)

        expected = [["_rowName","v.x","v.y"],["[]",3,2]]
        self.assertEqual(res, expected)

mldb.run_tests()
