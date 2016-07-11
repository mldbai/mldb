#
# MLDB-1779_column_expr.py
# Francois Maillet, 2016-07-06
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1779ColumnExpr(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        pass

    def test_it(self):
        self.assertTableResultEquals(
            mldb.query("SELECT column expr () from (select x.a:1, y.b:2)"),
            mldb.query("SELECT * from (select x.a:1, y.b:2)"))

    def test_columnPathElement(self):

        subselect = """select parse_json('{"age": 5, "friends": [{"name": "tommy"}, {"name": "sally"}]}') as *"""
        expected = [["_rowName","friends.0.name"],
                    ["result", "tommy"]]

        self.assertTableResultEquals(
            mldb.query("""
                select COLUMN EXPR (WHERE columnPathElement(1) = '0')
                FROM ( %s )
            """ % subselect),
            expected)

        self.assertTableResultEquals(
            mldb.query("""
                select COLUMN EXPR (WHERE columnPathElement(-2) = '0')
                FROM ( %s )
            """ % subselect),
            expected)

if __name__ == '__main__':
    mldb.run_tests()

