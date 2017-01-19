#
# MLDB-1668
# 2016-05-19 Francois Maillet
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1668Test(MldbUnitTest):  
    @classmethod
    def setUpClass(self):
        pass
    
    def test_index(self):
        def doTest(a, b, score):
            self.assertTableResultEquals(
                mldb.query("""
                    select jaccard_index(
                                tokenize('%s', {splitChars: ' .,'}),
                                tokenize('%s', {splitChars: ' .,'})
                    ) as jaccard
                """ % (a, b)),
                [
                    ["_rowName", "jaccard"],
                    [  "result", score ]
                ]
            )

        doTest('1234 king st., london, on', '1234 king street london, gb', 0.42857142857142855)
        doTest('hola amigo', 'chao amigo', 0.3333333333333333)
        doTest('', '', 1)
        doTest('a b c', 'x y z r', 0)
   
    def test_wrong_type(self):
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.query("select jaccard_index(2, 'adsf')") 

mldb.run_tests()

