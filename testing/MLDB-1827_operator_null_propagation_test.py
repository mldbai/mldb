#
# MLDB-1827_operator_null_propagation_test.py
# Francois-Michel L Heureux, 2016-07-18
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1827OperatorNullPropagationTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['val', 4, 6]])
        ds.commit()

    def run_operations(self, operator):
        res = mldb.get('/v1/query', q="SELECT val {} NULL FROM ds"
                       .format(operator)).json()[0]['columns'][0]
        self.assertEqual(res[1], None)
        self.assertEqual(res[2], '1970-01-01T00:00:06Z')

        res = mldb.get('/v1/query', q="SELECT NULL {} NULL FROM ds"
                       .format(operator)).json()[0]['columns'][0]
        self.assertEqual(res[1], None)
        self.assertEqual(res[2], '-Inf')

        res = mldb.get('/v1/query', q="SELECT NULL {} val FROM ds"
                       .format(operator)).json()[0]['columns'][0]
        self.assertEqual(res[1], None)
        self.assertEqual(res[2], '1970-01-01T00:00:06Z')

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
