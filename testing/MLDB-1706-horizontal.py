#
# MLDB-1706-horizontal.py
# Mathieu Marquis Bolduc, 2016-06-08
# Copyright (c) 2016 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1706Test(MldbUnitTest):  # noqa

    def test_single_val(self):
        self.assertTableResultEquals(
            mldb.query("select horizontal_min( {'a','b'} )"), 
            [["_rowName","horizontal_min( {'a','b'} )"],
             ["result","a"]])

        self.assertTableResultEquals(
            mldb.query("select horizontal_max ( {'a','b'} )"),
            [["_rowName","horizontal_max ( {'a','b'} )"],
             ["result","b"]])

        self.assertTableResultEquals(
            mldb.query("select horizontal_min( {TIMESTAMP 1, TIMESTAMP 2} )"),
            [["_rowName","horizontal_min( {TIMESTAMP 1, TIMESTAMP 2} )"],
             ["result","1970-01-01T00:00:01Z"]])

        self.assertTableResultEquals(
            mldb.query("select horizontal_max( {TIMESTAMP 1, TIMESTAMP 2} )"),
            [["_rowName","horizontal_max( {TIMESTAMP 1, TIMESTAMP 2} )"],[
              "result","1970-01-01T00:00:02Z"]])

if __name__ == '__main__':
    mldb.run_tests()
