#
# mldb_unit_test_test.py
# Mich, 2016-02-02
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
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
        query = "SELECT * FROM ds ORDER BY rowName()"
        cls.flat_res = mldb.query(query)
        cls.object_res = mldb.get('/v1/query', q=query).json()

    @property
    def flat_res(self):
        return self.__class__.flat_res

    @property
    def object_res(self):
        return self.__class__.object_res

    def test_flat_valid(self):
        expected = [["_rowName", "colB", "colA"],
                    ["row1", 1, 1],
                    ["row2", None, 1]]
        self.assertTableResultEquals(self.flat_res, expected)

    def test_flat_wrong_output(self):
        expected = [["_rowName", "colB", "colA"],
                    ["row1", 1, None],
                    ["row2", None, 1]]
        with self.assertRaises(AssertionError):
            self.assertTableResultEquals(self.flat_res, expected)

    def test_flat_unmatching_size(self):
        expected = []
        with self.assertRaises(AssertionError):
            self.assertTableResultEquals(self.flat_res, expected)

        with self.assertRaises(AssertionError):
            self.assertTableResultEquals(expected, self.flat_res)

        expected = [["_rowName", "colB", "colA"],
                    ["row1", 1, None]]
        with self.assertRaises(AssertionError):
            self.assertTableResultEquals(expected, self.flat_res)

    def test_flat_col_names_diff(self):
        expected = [["_rowName", "colB", "colC"],
                    ["row1", 1, 1],
                    ["row2", None, 1]]
        with self.assertRaises(AssertionError):
            self.assertTableResultEquals(self.flat_res, expected)

    def test_flat_col_names_size_diff(self):
        expected = [["_rowName", "colC", "colA", "colD"],
                    ["row1", 1, None, 1],
                    ["row2", None, 1, 1]]
        with self.assertRaises(AssertionError):
            self.assertTableResultEquals(self.flat_res, expected)

    def test_flat_unordered_rows(self):
        expected = [["_rowName", "colC", "colA", "colD"],
                    ["row2", None, 1],
                    ["row1", 1, 1]]
        with self.assertRaises(AssertionError):
            self.assertTableResultEquals(self.flat_res, expected)

    def test_object_valid(self):
        expected = [{"rowName":"row1","columns":[["colA",1,"1970-01-01T00:00:00Z"],
                                                 ["colA",1,"1970-01-01T00:00:02Z"],
                                                 ["colB",1,"1970-01-01T00:00:01Z"]]},
                    {"rowName":"row2","columns":[["colA",1,"1970-01-01T00:00:00Z"]]}]
        self.assertFullResultEquals(self.object_res, expected)

    def test_object_row_size_diff(self):
        expected = [{"rowName":"row1","columns":[["colA",1,"1970-01-01T00:00:00Z"],
                                                 ["colA",1,"1970-01-01T00:00:02Z"],
                                                 ["colB",1,"1970-01-01T00:00:01Z"]]},
                    {"rowName":"row2","columns":[["colA",1,"1970-01-01T00:00:00Z"]]},
                    {"rowName":"row3","columns":[["colA",1,"1970-01-01T00:00:00Z"]]}]
        with self.assertRaises(AssertionError):
            self.assertFullResultEquals(self.object_res, expected)

    def test_object_row_name_diff(self):
        expected = [{"rowName":"row1","columns":[["colA",1,"1970-01-01T00:00:00Z"],
                                                 ["colA",1,"1970-01-01T00:00:02Z"],
                                                 ["colB",1,"1970-01-01T00:00:01Z"]]},
                    {"rowName":"row3","columns":[["colA",1,"1970-01-01T00:00:00Z"]]}]
        with self.assertRaises(AssertionError):
            self.assertFullResultEquals(self.object_res, expected)

    def test_object_col_size_diff(self):
        expected = [{"rowName":"row1","columns":[["colA",1,"1970-01-01T00:00:00Z"],
                                                 ["colB",1,"1970-01-01T00:00:01Z"]]},
                    {"rowName":"row2","columns":[["colA",1,"1970-01-01T00:00:00Z"]]}]
        with self.assertRaises(AssertionError):
            self.assertFullResultEquals(self.object_res, expected)

    def test_object_col_content_diff(self):
        expected = [{"rowName":"row1","columns":[["colA",1,"1970-01-01T00:00:00Z"],
                                                 ["colA",2,"1970-01-01T00:00:02Z"],
                                                 ["colB",1,"1970-01-01T00:00:01Z"]]},
                    {"rowName":"row2","columns":[["colA",1,"1970-01-01T00:00:00Z"]]}]
        with self.assertRaises(AssertionError):
            self.assertFullResultEquals(self.object_res, expected)

    def test_object_unordered_rows(self):
        expected = [{"rowName":"row2","columns":[["colA",1,"1970-01-01T00:00:00Z"]]},
                    {"rowName":"row1","columns":[["colA",1,"1970-01-01T00:00:00Z"],
                                                 ["colA",1,"1970-01-01T00:00:02Z"],
                                                 ["colB",1,"1970-01-01T00:00:01Z"]]}]
        with self.assertRaises(AssertionError):
            self.assertFullResultEquals(self.object_res, expected)

mldb.run_tests()
