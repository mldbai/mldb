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

if __name__ == '__main__':
    mldb.run_tests()
