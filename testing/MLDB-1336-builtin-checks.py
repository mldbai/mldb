
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

mldb = mldb_wrapper.wrap(mldb) # noqa

import unittest

class myTest(unittest.TestCase):

    def test_sequence(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, "Executing builtin function exp: Can't convert value 'a' to double") as re:
            query = "SELECT exp('a')"
            mldb.query(query)
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, "Executing builtin function ln: ln function supports positive numbers only") as re:
            query = "SELECT ln(-1)"
            mldb.query(query)
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, "Binding builtin function sqrt: expected 1 argument, got 3") as re:
            query = "SELECT sqrt(1,2,3)"
            mldb.query(query)

mldb.run_tests()
