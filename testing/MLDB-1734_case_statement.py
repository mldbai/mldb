#
# MLDB-1734_case_statement.py
# Francois Maillet, 2016-06-16
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1734CaseStatement(MldbUnitTest):  # noqa

    expected_row = [
        ["_rowName", "y", "z"],
        ["b", None, None],
        ["a", 8, 5]
    ]

    expected_scalar = [
        ['_rowName', 'res'],
        ['b', None],
        ['a', 999]
    ]

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({"id": "sample", "type": "sparse.mutable"})
        ds.record_row("a",[["x", 1, 0]])
        ds.record_row("b",[["y", 1, 0]])
        ds.commit()

    def test_defined_else_as_row(self):


        # this query works
        self.assertTableResultEquals(
            mldb.query("""
                SELECT CASE x = 1
                        WHEN 1 THEN {z:5, y:8}
                        ELSE {}
                       END as *
                FROM sample
            """),
            self.expected_row)

    def test_default_else_should_be_row(self):
        self.assertTableResultEquals(
            mldb.query("""
                SELECT CASE x = 1
                        WHEN 1 THEN {z:5, y:8}
                       END as *
                FROM sample
            """),
            self.expected_row)

    def test_defined_else_as_scalar(self):
        self.assertTableResultEquals(
            mldb.query("""
                SELECT CASE x = 1
                        WHEN 1 THEN 999
                        ELSE NULL
                       END AS res
                FROM sample
            """),
            self.expected_scalar
        )

    def test_default_else_should_be_scalar(self):
        self.assertTableResultEquals(
            mldb.query("""
                SELECT CASE x = 1
                        WHEN 1 THEN 999
                       END AS res
                FROM sample
            """),
            self.expected_scalar
        )


if __name__ == '__main__':
    mldb.run_tests()
