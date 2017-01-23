#
# MLDB-2107-scalar-format.py
# Mathieu Marquis Bolduc, 2017-01-10
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2107ScalarFormatTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row0', [['x', 'A', 0]])
        ds.record_row('row1', [['x', 'B', 0]])
        ds.commit()
     
    def test_int(self):
        n = mldb.get('/v1/query', q="select x from (select 17 as x)", format='atom').json()
        self.assertEqual(17, n)

    def test_float(self):
        n = mldb.get('/v1/query', q="select x from (select 2.3 as x)", format='atom').json()
        self.assertEqual(2.3, n)

    def test_string(self):
        n = mldb.get('/v1/query', q="select x from (select 'blah' as x)", format='atom').json()
        self.assertEqual('blah', n)

    def test_bool(self):
        n = mldb.get('/v1/query', q="select x from (select false as x)", format='atom').json()
        self.assertEqual(False, n)

    def test_error_columns(self):
        msg = "Query with atom format returned multiple columns"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            n = mldb.get('/v1/query', q="select x,y from (select false as x, 1 as y)", format='atom').json()

    def test_error_rows(self):
        msg = "Query with atom format returning multiple rows"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            n = mldb.get('/v1/query', q="select x from ds", format='atom').json()

    def test_multiple_rows_limit(self):
        n = mldb.get('/v1/query', q="select x from ds limit 1", format='atom').json()
        self.assertEqual('B', n)

    def test_error_no_rows(self):
        msg = "Query with atom format returned no rows."
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            n = mldb.get('/v1/query', q="select x from ds where x = 'patate'", format='atom').json()

    def test_error_no_column(self):
        msg = "Query with atom format returned no column"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            n = mldb.get('/v1/query', q="select COLUMN EXPR (WHERE columnName() IN ('Z')) from (select 17 as x)", format='atom').json()

if __name__ == '__main__':
    mldb.run_tests()
