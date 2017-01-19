#
# MLDB-1732-columnCount_in_where.py
# 16 juin 2016, Francois Maillet
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1732(MldbUnitTest):  # noqa
    @classmethod
    def setUpClass(self):
        ds = mldb.create_dataset({"id": "sample", "type": "sparse.mutable"})
        ds.record_row("a", [["x", 1, 0]])
        ds.record_row("b", [])
        ds.commit()

    def test_no_table_error_message(self):
        msg = 'Wildcard usage requires a FROM statement'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT *")

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT * WHERE columnCount() > 0")

    def test_where_columncount_works(self):
        self.assertTableResultEquals(
            mldb.query("SELECT * FROM sample WHERE columnCount() > 0"),
            [
                ["_rowName", "x"],
                [       "a",  1 ]
            ]
        )

mldb.run_tests()
