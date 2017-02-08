#
# total_rows_builtin_fct_test.py
# Francois-Michel L Heureux, 2016-07-29
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
if False:
    mldb_wrapper = None

mldb = mldb_wrapper.wrap(mldb)  # noqa

class TotalRowsBuiltinFctTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        for i in xrange(4):
            ds.record_row(i, [['col', i, 0]])
        ds.commit()

    def test_no_order(self):
        res = mldb.query("SELECT totalRows() FROM ds")
        self.assertEqual(res[1][1], 4)
        self.assertEqual(res[2][1], 4)
        self.assertEqual(res[3][1], 4)
        self.assertEqual(res[4][1], 4)

    def test_no_order_with_operator(self):
        res = mldb.query("SELECT totalRows() - 1 FROM ds")
        self.assertEqual(res[1][1], 3)
        self.assertEqual(res[2][1], 3)
        self.assertEqual(res[3][1], 3)
        self.assertEqual(res[4][1], 3)

    def test_asc_order(self):
        res = mldb.query(
            "SELECT totalRows() AS total FROM ds ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'total'],
            ['0', 4],
            ['1', 4],
            ['2', 4],
            ['3', 4]
        ])

    def test_desc_order(self):
        res = mldb.query(
            "SELECT totalRows() AS total FROM ds ORDER BY rowName() DESC")
        self.assertTableResultEquals(res, [
            ['_rowName', 'total'],
            ['3', 4],
            ['2', 4],
            ['1', 4],
            ['0', 4]
        ])

    def test_where(self):
        res = mldb.query("SELECT totalRows() FROM ds WHERE rowName() < '2'")
        self.assertEqual(len(res), 3)
        self.assertEqual(res[1][1], 2)
        self.assertEqual(res[2][1], 2)

        res = mldb.query("SELECT totalRows() FROM ds WHERE rowName() >= '2'")
        self.assertEqual(len(res), 3)
        self.assertEqual(res[1][1], 2)
        self.assertEqual(res[2][1], 2)

    def test_limit(self):
        res = mldb.query(
            "SELECT totalRows() FROM ds ORDER BY rowName() LIMIT 2")
        self.assertEqual(len(res), 3)
        self.assertEqual(res[1][1], 4)
        self.assertEqual(res[2][1], 4)

    def test_limit_and_offset(self):
        res = mldb.query(
            "SELECT totalRows() FROM ds ORDER BY rowName() LIMIT 2 OFFSET 2")
        self.assertEqual(len(res), 3)
        self.assertEqual(res[1][1], 4)
        self.assertEqual(res[2][1], 4)

    def test_where_row_number(self):
        msg = "function totalRows is only available in SELECT expressions."
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT * FROM ds WHERE totalRows() < 2")

    def test_order_by_row_number(self):
        msg = "function totalRows is only available in SELECT expressions."
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT * FROM ds ORDER BY totalRows()")

    def test_group_by_row_number(self):
        msg = "function totalRows is only available in SELECT expressions."
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT 'coco' FROM ds GROUP BY totalRows()")

    def test_named_row_number(self):
        msg = "function totalRows is only available in SELECT expressions."
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT rowName() NAMED totalRows() FROM ds")

if __name__ == '__main__':
    mldb.run_tests()
