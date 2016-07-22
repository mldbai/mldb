#
# MLDB-1833_row_expr_equality_operator_test.py
# Francois-Michel L Heureux, 2016-07-22
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1833RowExprEqualityOperatorTest(MldbUnitTest):  # noqa

    def test_it(self):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row', [['a', 1, 0], ['b', 2, 0]])
        ds.commit()

        res = mldb.query("SELECT {*} + 1 FROM ds")
        mldb.log(res)
        res = mldb.query("SELECT {*} = {*} FROM ds")
        res = mldb.query("SELECT {*} = {a, b} FROM ds")
        res = mldb.query("SELECT {*} = {b, a} FROM ds")
        res = mldb.query("SELECT {*} = {a, a, b} FROM ds")
        mldb.log(res)

if __name__ == '__main__':
    mldb.run_tests()
