#
# MLDB-1779_column_expr.py
# Francois Maillet, 2016-07-06
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1779ColumnExpr(MldbUnitTest):  # noqa
    @classmethod
    def setUpClass(cls):
        mldb.put('/v1/datasets/example', { "type":"sparse.mutable" })

        mldb.post('/v1/datasets/example/rows', {
            "rowName": "first row",
            "columns": [
                ['"as_chat.""topics.Bullying"""', 1, 0],
                ['"as_chat.""topics.Junk"""', 2, 0]
            ]
        })

        mldb.post('/v1/datasets/example/rows', {
            "rowName": "second row",
            "columns": [
                ["pwet", 3, 0],
                ['"as_chat.""topics.Junk"""', 4, 0]
            ]
        })
        mldb.post("/v1/datasets/example/commit")

    def test_columnPathElem(self):
        msg = "Cannot have a NULL column name"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query('''
                select COLUMN EXPR (AS columnPathElement(1)
                    WHERE columnName() LIKE '%topics%Junk%') from example
            ''')

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

