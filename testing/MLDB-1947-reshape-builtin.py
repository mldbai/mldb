#
# MLDB-1947-reshape-builtin.py
# Mathieu Marquis Bolduc, 2016-09-15
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1947reshapebuiltin(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        pass

    def test_reshape(self):
        res = mldb.query("SELECT shape([[1,2],[3,4]]) as dim")

        expected = [["_rowName", "dim.0", "dim.1"],
                    ["result", 2, 2]]

        self.assertTableResultEquals(res, expected)

        res = mldb.query("SELECT shape(reshape([[1,2],[3,4]], [2,2])) as dim")
        self.assertTableResultEquals(res, expected)

        res = mldb.query("SELECT shape(reshape([[1,2],[3,4]], [4])) as dim")

        expected = [["_rowName", "dim.0"],
                    ["result", 4]]

        self.assertTableResultEquals(res, expected)

        res = mldb.query("SELECT shape([1,2,3,4]) as dim")
        self.assertTableResultEquals(res, expected)

        res = mldb.query("SELECT shape(reshape([1,2,3,4], [2,2])) as dim")

        expected = [["_rowName", "dim.0", "dim.1"],
                    ["result", 2, 2]]

        self.assertTableResultEquals(res, expected)

    def test_reshape_enlarge(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                     'Attempt to enlarge embedding by resizing'):
            mldb.query("SELECT shape(reshape([1,2,3,4,5], [2,2])) as dim")

if __name__ == '__main__':
    mldb.run_tests()
