#
# sign_function_test.py
# Francois-Michel L Heureux, 2016-06-21
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class SignFunctionTest(MldbUnitTest):  # noqa

    def test_negative_int(self):
        res = mldb.query("SELECT sign(-123)")
        self.assertEqual(res[1][1], -1)

    def test_positive_int(self):
        res = mldb.query("SELECT sign(123)")
        self.assertEqual(res[1][1], 1)

    def test_zero_int(self):
        res = mldb.query("SELECT sign(0)")
        self.assertEqual(res[1][1], 0)

    def test_negative_float(self):
        res = mldb.query("SELECT sign(-123.123)")
        self.assertEqual(res[1][1], -1)

    def test_positive_float(self):
        res = mldb.query("SELECT sign(123.123)")
        self.assertEqual(res[1][1], 1)

    def test_zero_float(self):
        res = mldb.query("SELECT sign(0.0)")
        self.assertEqual(res[1][1], 0)

    def test_nan(self):
        res = mldb.query("SELECT sign(nan)")
        self.assertEqual(res[1][1], "NaN")

    def test_string(self):
        res = mldb.query("SELECT sign('octosanchez')")
        self.assertEqual(res[1][1], "NaN")

    def test_null(self):
        res = mldb.query("SELECT sign(NULL)")
        self.assertEqual(res[1][1], None)

if __name__ == '__main__':
    mldb.run_tests()
