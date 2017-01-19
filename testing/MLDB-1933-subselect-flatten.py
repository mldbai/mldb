#
# MLDB-1933-subselect-flatten.py
# Mathieu Marquis Bolduc, 2016-09-08
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class ColumnExprTest(MldbUnitTest):  # noqa

    def test_subselect(self):
        res = mldb.query("SELECT COLUMN EXPR STRUCTURED (SELECT 1) FROM (SELECT [[2,3],[4,5]] as myembedding)")
        expected = [["_rowName","myembedding"],["result",1]]
        self.assertTableResultEquals(res, expected);

    def test_subselect_multiple(self):
        res = mldb.query("SELECT COLUMN EXPR STRUCTURED (SELECT 1) FROM (SELECT [2,3] as x,[4,5] as y)")
        expected = [["_rowName", "x", "y"],
                    ["result", 1, 1]]
        self.assertTableResultEquals(res, expected);

    def test_subselect_multiple_value(self):
        res = mldb.query("SELECT COLUMN EXPR STRUCTURED (SELECT norm(value(), 2)) FROM (SELECT [2,3] as x,[4,5] as y)")
        expected = [["_rowName", "x", "y"],
                    ["result", 3.605551275463989, 6.4031242374328485]]

if __name__ == '__main__':
    mldb.run_tests()    
