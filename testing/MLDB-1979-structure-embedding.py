#
# MLDB-1979-structure-embedding.py
# Mathieu Marquis Bolduc, 2016-09-08
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class StructureEmbeddingTest(MldbUnitTest):  # noqa

    def test_subselect(self):
        res = mldb.query("SELECT COLUMN EXPR STRUCTURED(select tf_EncodePng({image : value()})) FROM (SELECT [[[[1,2,3],[2,3,4]],[[3,4,5],[4,5,6]]]] as *)")
        expected = [["_rowName", "0"], [ "result", {
                "blob": [137, "PNG", 13, 10, 26, 10, 0, 0, 0, 13, "IHDR", 0, 0, 0, 2, 0, 0, 0, 2, 8, 2, 0, 0, 0, 253, 212, 154, 115, 0, 0, 0, 22, "IDAT", 8, 153, "cddbfdddabbbdd", 4, 0, 0, 189, 0, 24, 130, 2, 132, 40, 0, 0, 0, 0, "IEND", 174, "B`", 130 ] }
                    ]]
        self.assertEqual(res, expected)

if __name__ == '__main__':
    mldb.run_tests()    
