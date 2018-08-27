
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

from mldb import mldb, ResponseException

import unittest

class myTest(unittest.TestCase):

    def test_sequence(self):
        with self.assertRaisesRegex(ResponseException, "Executing builtin function exp: Can't convert value 'a' of type 'ASCII_STRING' to double") as re:
            query = "SELECT exp('a')"
            mldb.query(query)
        with self.assertRaisesRegex(ResponseException, "Binding builtin function sqrt: expected 1 argument, got 3") as re:
            query = "SELECT sqrt(1,2,3)"
            mldb.query(query)

request.set_return(mldb.run_tests())
