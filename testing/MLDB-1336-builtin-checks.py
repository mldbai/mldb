
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

mldb = mldb_wrapper.wrap(mldb) # noqa

import unittest

class myTest(unittest.TestCase):

    def test_sequence(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, "Executing builtin function exp: Can't convert value 'a' of type 'ASCII_STRING' to double") as re:
            query = "SELECT exp('a')"
            mldb.query(query)
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, "Binding builtin function sqrt: expected 1 argument, got 3") as re:
            query = "SELECT sqrt(1,2,3)"
            mldb.query(query)

mldb.run_tests()
