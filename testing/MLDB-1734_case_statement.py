#
# MLDB-1734_case_statement.py
# Francois Maillet, 2016-06-16
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1734CaseStatement(MldbUnitTest):

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        ds.record_row("a",[["x", 1, 0]])
        ds.record_row("b",[["y", 1, 0]])
        ds.commit()

    def test_it(self):

        good_answer = [
            ["_rowName", "y", "z"],
            ["b", None, None],
            ["a", 8, 5]
        ]

        # this query works
        self.assertTableResultEquals(
            mldb.query("""
                select CASE x = 1
                        WHEN 1 THEN {z:5, y:8}
                        ELSE {}
                       END as *
                FROM sample
            """),
            good_answer)

        self.assertTableResultEquals(
            mldb.query("""
                select CASE x = 1
                        WHEN 1 THEN {z:5, y:8}
                       END as *
                FROM sample
            """),
            good_answer)
        

if __name__ == '__main__':
    mldb.run_tests()
