#
# MLDB-1827_operator_null_propagation_test.py
# Francois-Michel L Heureux, 2016-07-18
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1827OperatorNullPropagationTest(MldbUnitTest):  # noqa

    def run_operations(self, operator):
        res = mldb.query("SELECT 4 {} NULL".format(operator))
        self.assertEqual(res[1][1], None)

        res = mldb.query("SELECT NULL {} NULL".format(operator))
        self.assertEqual(res[1][1], None)

        res = mldb.query("SELECT NULL {} 4".format(operator))
        self.assertEqual(res[1][1], None)

    def test_plus(self):
        self.run_operations('+')

    def test_minus(self):
        self.run_operations('-')

    def test_modulus(self):
        self.run_operations('%')

    def test_division(self):
        self.run_operations('/')

    def test_multiplication(self):
        self.run_operations('*')


if __name__ == '__main__':
    mldb.run_tests()
