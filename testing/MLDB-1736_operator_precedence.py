#
# MLDB-1736_operator_precedence.py
# Francois Maillet, 2016-06-17
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1736OperatorPrecedence(MldbUnitTest):
    @classmethod
    def setUpClass(cls):
        pass

    def test_in(self):
        self.assertTableResultEquals(
            mldb.query("""
                SELECT
                    'a' IN ('a') AND 'b' IN ('b') as colA,
                    ('a' IN ('a')) AND ('b' IN ('b')) as colB
            """),
            [
                ["_rowName", "colA", "colB"],
                [  "result",  1, 1]
            ]
        )

    def test_like(self):
        self.assertTableResultEquals(
            mldb.query("""
                SELECT
                    'a' LIKE 'a' AND 'b' LIKE 'b' as colA,
                    ('a' LIKE 'a') AND ('b' LIKE 'b') as colB
            """),
            [
                ["_rowName", "colA", "colB"],
                [  "result",  1, 1]
            ]
        )

if __name__ == '__main__':
    mldb.run_tests()
