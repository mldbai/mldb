#
# MLDB-2142-prefix-suffix.py
# Mathieu Marquis Bolduc, 2017-02-10
# This file is part of MLDB. Copyright 2017 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2077MergeTest(MldbUnitTest):  # noqa

    def test_prefix(self):
        n = mldb.get('/v1/query', q="SELECT remove_prefix('awesome', 'awe')", format='atom').json()
        self.assertEqual('some', n)
    def test_prefix_not(self):
        n = mldb.get('/v1/query', q="SELECT remove_prefix('awesome', 'eso')", format='atom').json()
        self.assertEqual('awesome', n)
    def test_suffix(self):
        n = mldb.get('/v1/query', q="SELECT remove_suffix('awesome', 'some')", format='atom').json()
        self.assertEqual('awe', n)
    def test_suffix_not(self):
        n = mldb.get('/v1/query', q="SELECT remove_suffix('awesome', 'eso')", format='atom').json()
        self.assertEqual('awesome', n)
    def test_prefix_arg(self):
        msg = 'The arguments passed to remove_prefix must be two strings'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            n = mldb.get('/v1/query', q="SELECT remove_prefix('awesome', 2)", format='atom').json()
    def test_suffix_arg(self):
        msg = 'The arguments passed to remove_suffix must be two strings'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            n = mldb.get('/v1/query', q="SELECT remove_suffix(2, 'eso')", format='atom').json()    

if __name__ == '__main__':
    mldb.run_tests()
