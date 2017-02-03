#
# MLDB-1947-reshape-builtin.py
# Mathieu Marquis Bolduc, 2016-09-15
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
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
                                     'Attempt to change embedding size by reshaping'):
            mldb.query("SELECT shape(reshape([1,2,3,4,5], [2,2])) as dim")

    def test_reshape_not_embedding(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                     'Null embedding'):
            mldb.query("SELECT shape(reshape('not an embedding', [1])) as dim")

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                     'requires an embedding'):
            mldb.query("SELECT shape(reshape([1], 'not an embedding')) as dim")

    def test_reshape_row(self):
        res = mldb.query('SELECT reshape({"0": 1, "1": 2, "2": 3, "3": 4}, [2, 2]) as *')

        expected = [["_rowName", "0.0", "0.1", "1.0", "1.1"],
                    ["result", 1, 2, 3, 4]]

        self.assertTableResultEquals(res, expected)

        res = mldb.query('SELECT reshape({"0": 1, "1": 2, "2": 3, "3": 4}, [1, 4]) as *')

        expected = [["_rowName", "0.0", "0.1", "0.2", "0.3"],
                    ["result", 1, 2, 3, 4]]

        self.assertTableResultEquals(res, expected)


        res = mldb.query('SELECT reshape({"0": {"0": 1, "1": 2}, "1": {"0": 3, "1": 4}}, [4]) as *')
        
        expected = [["_rowName", "0", "1", "2", "3"],
                    ["result", 1, 2, 3, 4]]

        self.assertTableResultEquals(res, expected)

        res = mldb.query('SELECT reshape({"0": {"0": 1, "1": 2}, "1": {"0": 3, "1": 4}}, [1, 4]) as *')
        
        expected = [["_rowName", "0.0", "0.1", "0.2", "0.3"],
                    ["result", 1, 2, 3, 4]]

        self.assertTableResultEquals(res, expected)

if __name__ == '__main__':
    mldb.run_tests()
