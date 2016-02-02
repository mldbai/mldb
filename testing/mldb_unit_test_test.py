#
# mldb_unit_test_test.py
# Mich, 2016-02-02
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

class MldbUnitTestTest(MldbUnitTest): # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({
            'id' : 'ds',
            'type' : 'sparse.mutable'
        })

        ds.record_row('row1', [['colA', 1, 0], ['colA', 1, 2], ['colB', 1, 1]])
        ds.record_row('row2', [['colA', 1, 0]])
        ds.commit()

    def test_flat_valid(self):
        res = mldb.query("SELECT * FROM ds")
        expected = [["_rowName", "colB", "colA"],
                    ["row2", None, 1],
                    ["row1", 1, 1]]
        self.assertQueryResult(res, expected)

    def test_flat_wrong_output(self):
        res = mldb.query("SELECT * FROM ds")
        expected = [["_rowName", "colB", "colA"],
                    ["row2", None, 1],
                    ["row1", 1, None]]
        with self.assertRaises(AssertionError):
            self.assertQueryResult(res, expected)

    def test_flat_unmatching_size(self):
        res = mldb.query("SELECT * FROM ds")
        expected = []
        with self.assertRaises(AssertionError):
            self.assertQueryResult(res, expected)

        with self.assertRaises(AssertionError):
            self.assertQueryResult(expected, res)

        expected = [["_rowName", "colB", "colA"],
                    ["row1", 1, None]]
        with self.assertRaises(AssertionError):
            self.assertQueryResult(expected, res)

    def test_flat_col_names_diff(self):
        res = mldb.query("SELECT * FROM ds")
        expected = [["_rowName", "colC", "colA"],
                    ["row2", None, 1],
                    ["row1", 1, None]]
        with self.assertRaises(AssertionError):
            self.assertQueryResult(res, expected)

    def test_flat_col_names_size_diff(self):
        res = mldb.query("SELECT * FROM ds")
        expected = [["_rowName", "colC", "colA", "colD"],
                    ["row2", None, 1, 1],
                    ["row1", 1, None, 1]]
        with self.assertRaises(AssertionError):
            self.assertQueryResult(res, expected)

    def test_object_valid(self):
        res = mldb.get('/v1/query', q="SELECT * FROM ds").json()
        expected = [{"rowName":"row1","columns":[["colA",1,"1970-01-01T00:00:00Z"],
                                                 ["colA",1,"1970-01-01T00:00:02Z"],
                                                 ["colB",1,"1970-01-01T00:00:01Z"]]},
                    {"rowName":"row2","columns":[["colA",1,"1970-01-01T00:00:00Z"]]}]
        self.assertQueryResult(res, expected)

    def test_object_row_size_diff(self):
        res = mldb.get('/v1/query', q="SELECT * FROM ds").json()
        expected = [{"rowName":"row1","columns":[["colA",1,"1970-01-01T00:00:00Z"],
                                                 ["colA",1,"1970-01-01T00:00:02Z"],
                                                 ["colB",1,"1970-01-01T00:00:01Z"]]},
                    {"rowName":"row2","columns":[["colA",1,"1970-01-01T00:00:00Z"]]},
                    {"rowName":"row3","columns":[["colA",1,"1970-01-01T00:00:00Z"]]}]
        with self.assertRaises(AssertionError):
            self.assertQueryResult(res, expected)

    def test_object_row_name_diff(self):
        res = mldb.get('/v1/query', q="SELECT * FROM ds").json()
        expected = [{"rowName":"row1","columns":[["colA",1,"1970-01-01T00:00:00Z"],
                                                 ["colA",1,"1970-01-01T00:00:02Z"],
                                                 ["colB",1,"1970-01-01T00:00:01Z"]]},
                    {"rowName":"row3","columns":[["colA",1,"1970-01-01T00:00:00Z"]]}]
        with self.assertRaises(AssertionError):
            self.assertQueryResult(res, expected)

    def test_object_col_size_diff(self):
        res = mldb.get('/v1/query', q="SELECT * FROM ds").json()
        expected = [{"rowName":"row1","columns":[["colA",1,"1970-01-01T00:00:00Z"],
                                                 ["colB",1,"1970-01-01T00:00:01Z"]]},
                    {"rowName":"row2","columns":[["colA",1,"1970-01-01T00:00:00Z"]]}]
        with self.assertRaises(AssertionError):
            self.assertQueryResult(res, expected)

    def test_object_col_content_diff(self):
        res = mldb.get('/v1/query', q="SELECT * FROM ds").json()
        expected = [{"rowName":"row1","columns":[["colA",1,"1970-01-01T00:00:00Z"],
                                                 ["colA",2,"1970-01-01T00:00:02Z"],
                                                 ["colB",1,"1970-01-01T00:00:01Z"]]},
                    {"rowName":"row2","columns":[["colA",1,"1970-01-01T00:00:00Z"]]}]
        with self.assertRaises(AssertionError):
            self.assertQueryResult(res, expected)

mldb.run_tests()
