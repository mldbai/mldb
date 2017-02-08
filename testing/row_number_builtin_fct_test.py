#
# row_number_builtin_fct_test.py
# Francois-Michel L Heureux, 2016-07-20
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
if False:
    mldb_wrapper = None

mldb = mldb_wrapper.wrap(mldb)  # noqa

class RowNumberBuiltinFctTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        for i in xrange(4):
            ds.record_row(i, [['col', i, 0]])
        ds.commit()

    def test_no_order(self):
        res = mldb.query("SELECT rowNumber() FROM ds")
        self.assertEqual(res[1][1], 1)
        self.assertEqual(res[2][1], 2)
        self.assertEqual(res[3][1], 3)
        self.assertEqual(res[4][1], 4)

    def test_no_order_with_operator(self):
        res = mldb.query("SELECT rowNumber() -1 FROM ds")
        self.assertEqual(res[1][1], 0)
        self.assertEqual(res[2][1], 1)
        self.assertEqual(res[3][1], 2)
        self.assertEqual(res[4][1], 3)

    def test_asc_order(self):
        res = mldb.query(
            "SELECT rowNumber() AS num FROM ds ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'num'],
            ['0', 1],
            ['1', 2],
            ['2', 3],
            ['3', 4]
        ])

    def test_desc_order(self):
        res = mldb.query(
            "SELECT rowNumber() AS num FROM ds ORDER BY rowName() DESC")
        self.assertTableResultEquals(res, [
            ['_rowName', 'num'],
            ['3', 1],
            ['2', 2],
            ['1', 3],
            ['0', 4]
        ])

    def test_where(self):
        res = mldb.query("SELECT rowNumber() FROM ds WHERE rowName() < '2'")
        self.assertEqual(len(res), 3)
        self.assertEqual(res[1][1], 1)
        self.assertEqual(res[2][1], 2)

        res = mldb.query("SELECT rowNumber() FROM ds WHERE rowName() >= '2'")
        self.assertEqual(len(res), 3)
        self.assertEqual(res[1][1], 1)
        self.assertEqual(res[2][1], 2)

    def test_limit(self):
        res = mldb.query(
            "SELECT rowNumber() FROM ds ORDER BY rowName() LIMIT 2")
        self.assertEqual(len(res), 3)
        self.assertEqual(res[1][1], 1)
        self.assertEqual(res[2][1], 2)

    def test_limit_and_offset(self):
        res = mldb.query(
            "SELECT rowNumber() FROM ds ORDER BY rowName() LIMIT 2 OFFSET 2")
        self.assertEqual(len(res), 3)
        self.assertEqual(res[1][1], 3)
        self.assertEqual(res[2][1], 4)

    def test_where_row_number(self):
        msg = "function rowNumber is only available in SELECT expressions."
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT * FROM ds WHERE rowNumber() < 2")

    def test_order_by_row_number(self):
        msg = "function rowNumber is only available in SELECT expressions."
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT * FROM ds ORDER BY rowNumber()")

    def test_group_by_row_number(self):
        msg = "function rowNumber is only available in SELECT expressions."
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT 'coco' FROM ds GROUP BY rowNumber()")

    def test_named_row_number(self):
        msg = "function rowNumber is only available in SELECT expressions."
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT rowName() NAMED rowNumber() FROM ds")

if __name__ == '__main__':
    mldb.run_tests()
