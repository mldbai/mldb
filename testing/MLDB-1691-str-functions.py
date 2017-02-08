# -*- coding: utf-8 -*-
#
# MLDB-1691-str-functions.py
# Francois Maillet, 27 mai 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
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
 
    def test_length(self):
        self.doTest("select length('abcde') as rez", 5)
        self.doTest("select length('abcdéç') as rez", 6)
        self.doTest("select length(22) as rez", 2)

    def test_upper_lower(self):
        self.doTest("select upper('abcde') as rez", "ABCDE")
        self.doTest("select lower('ABCDe') as rez", "abcde")

mldb.run_tests()

