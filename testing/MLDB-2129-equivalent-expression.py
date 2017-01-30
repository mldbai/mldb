#
# MLDB-2129-equivalent-expression.py
# Mathieu Marquis Bolduc, 2017-01-27
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2126exportstructuredTest(MldbUnitTest):  # noqa
    """
    def test_trivial(self):
        n = mldb.get('/v1/query', q="select __isEquivalent(1, 1)", format='atom').json()
        self.assertEqual(True, n)

    def test_operator(self):
        n = mldb.get('/v1/query', q="select __isEquivalent(x+1, x+1) FROM (SELECT x : 1)", format='atom').json()
        self.assertEqual(True, n)

    def test_function(self):
        n = mldb.get('/v1/query', q="select __isEquivalent(sqrt(x+1), sqrt(x+1)) FROM (SELECT x : 1)", format='atom').json()
        self.assertEqual(True, n)
    """
    def test_commutative(self):
        n = mldb.get('/v1/query', q="select __isEquivalent(x+1, 1+x) FROM (SELECT x : 1)", format='atom').json()
        self.assertEqual(True, n) 

if __name__ == '__main__':
    mldb.run_tests()
