#
# MLDB-1935-const-binding.py
# Mathieu Marquis Bolduc, 2017-12-08
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2035ConstTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds1', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['a', 1, 0]])
        ds.commit()

    def test_var(self):
        res = mldb.query("SELECT __isconst(a) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

    def test_const(self):
        res = mldb.query("SELECT __isconst(1) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

    def test_const_comp_var(self):
        res = mldb.query("SELECT __isconst(a < 1) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

    def test_const_comp_const(self):
        res = mldb.query("SELECT __isconst(1 < 2) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

    def test_const_arith_var(self):
        res = mldb.query("SELECT __isconst(a + 1) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

    def test_const_arith_const(self):
        res = mldb.query("SELECT __isconst(1 + 2) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

    def test_const_unary_var(self):
        res = mldb.query("SELECT __isconst(-a) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

    def test_const_unary_const(self):
        res = mldb.query("SELECT __isconst(-(2)) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

    def test_const_bitwise_var(self):
        res = mldb.query("SELECT __isconst(a & 12345) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

    def test_const_bitwise_const(self):
        res = mldb.query("SELECT __isconst(12345 & 54321) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

    def test_const_unary_bitwise_var(self):
        res = mldb.query("SELECT __isconst(~a) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

    def test_const_unary_bitwise_const(self):
        res = mldb.query("SELECT __isconst(~12345) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

    def test_const_embedding_var(self):
        res = mldb.query("SELECT __isconst([1,a,3]) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

    def test_const_embedding_const(self):
        res = mldb.query("SELECT __isconst([1,2,3]) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

    def test_const_or_var(self):
        res = mldb.query("SELECT __isconst(a OR false) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

        res = mldb.query("SELECT __isconst(false OR a) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

        res = mldb.query("SELECT __isconst(a OR a) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

    def test_const_or_const(self):
        res = mldb.query("SELECT __isconst(true OR false) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

        res = mldb.query("SELECT __isconst(true OR a) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

        res = mldb.query("SELECT __isconst(a OR true) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

        res = mldb.query("SELECT __isconst(null OR a) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

        res = mldb.query("SELECT __isconst(a OR null) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

    def test_const_and_const(self):
        res = mldb.query("SELECT __isconst(true AND false) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

        res = mldb.query("SELECT __isconst(false AND a) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

        res = mldb.query("SELECT __isconst(a AND false) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

        res = mldb.query("SELECT __isconst(null AND a) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

        res = mldb.query("SELECT __isconst(a AND null) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])

    def test_const_and_var(self):
        res = mldb.query("SELECT __isconst(a AND true) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

        res = mldb.query("SELECT __isconst(true AND a) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

        res = mldb.query("SELECT __isconst(a AND a) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

    def test_const_not_var(self):
        res = mldb.query("SELECT __isconst(NOT a) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', False],
        ])

    def test_const_not_const(self):
        res = mldb.query("SELECT __isconst(NOT TRUE) as isconst FROM ds1 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'isconst',],
            ['row1', True],
        ])
     

if __name__ == '__main__':
    mldb.run_tests()
