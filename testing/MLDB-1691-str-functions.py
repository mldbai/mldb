#
# MLDB-1691-str-functions.py
# Francois Maillet, 27 mai 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1691(MldbUnitTest):  
    @classmethod
    def setUpClass(self):
        pass
    
    def doTest(self, query, rez):
        self.assertTableResultEquals(
            mldb.query(query),
            [
                ["_rowName", "rez"],
                [  "result",  rez ]
            ]
        )
 
    def test_str_length(self):
        self.doTest("select str_length('abcde') as rez", 5)
        self.doTest("select str_length('abcdéç') as rez", 6)

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                  'must be a string'):
            mldb.query("SELECT str_length(5) as length")

    def test_upper_lower(self):
        self.doTest("select upper('abcde') as rez", "ABCDE")
        self.doTest("select lower('ABCDe') as rez", "abcde")

mldb.run_tests()

