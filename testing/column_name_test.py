#
# column_name_test.py
# Francois-Michel L Heureux, 2016-07-21
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class ColumnNameTest(MldbUnitTest):  # noqa

    def select(self, select, expected):
        res = mldb.query("SELECT {}".format(select))
        self.assertEqual(res[0][1], expected)

    def select_err(self, select):
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.query("SELECT {}".format(select))

    def test_int(self):
        self.select(1, '1')

    def test_str(self):
        self.select("'patate'", "'patate'")

    def test_int_arith(self):
        self.select('1 + 10', '1 + 10')
        self.select('a:1 + 10', 'a')

    def test_float_arith(self):
        self.select('1 + 1.1', '"1 + 1.1"')
        self.select('a:1 + 1.1', 'a')

    def test_col_arith(self):
        self.select('a + b FROM (SELECT a:1, b:2)', 'a + b')

    def test_as(self):
        self.select('x:1', 'x')
        self.select('1 AS x', 'x')

        self.select("x.y:1 + 1", 'x.y')
        self.select("1 + 1 AS x.y", 'x.y')

    def test_dotted_as(self):
        self.select('1 as a.b', 'a.b')

    def test_object(self):
        self.select("{x:1}", '{x:1}.x')

    def test_object_as_star(self):
        self.select("{x:1} AS *", 'x')

    def test_object_as_x(self):
        self.select("{x:1} AS x", 'x.x')

    def test_object_over_object_arith(self):
        self.select("{x:1} + {x:1}", '{x:1} + {x:1}.x')

    def test_object_extraction(self):
        self.select("col.x FROM (SELECT col.x:1)", 'col.x')
        self.select("* FROM (SELECT col.x:1)", 'col.x')
        self.select("col.* FROM (SELECT col.x:1)", 'col.x')

    def test_object_extrac_star(self):
        self.select("c* FROM (SELECT col.x:1)", 'col.x')
        self.select("c* AS * FROM (SELECT col.x:1)", 'x')
        self.select("* AS * FROM (SELECT col.x:1)", 'col.x')

    def test_extract_all(self):
        self.select("{*} FROM (SELECT col.x:1)", '{*}.col.x')
        self.select("{*} AS * FROM (SELECT col.x:1)", 'col.x')

    def test_object_arith(self):
        self.select('{b:1} + 1', '{b:1} + 1.b')
        self.select('{b:1} + 1 AS *', 'b')

        self.select('a:{b:1} + 1', 'a.b')
        self.select_err('a:{b:1} + 1 AS *')

        self.select("{x.y:1 + 1}", '"{x.y:1 + 1}".x.y')
        self.select("{x.y:1 + 1} AS *", 'x.y')
        self.select("{{x.y:1 + 1} AS *} AS *", 'x.y')
        self.select_err("{x.y:1 + 1} AS x*")
        self.select_err("{x.y:1 + 1} AS x.*")

        self.select("{x:{y:1} + 1}", '{x:{y:1} + 1}.x.y')
        self.select("{x:{y:1} + 1} AS *", 'x.y')
        self.select_err("{x:{y:1} + 1} AS x*")
        self.select_err("{x:{y:1} + 1} AS x.*")

        # MLDB-1836
        self.select("{x.y:1} + 1", '"{x.y:1} + 1".x.y')
        self.select("{x:{y:1}} - 1", '{x:{y:1}} - 1.x.y')

        self.select("a:{x.y:1} * 1", 'a.x.y')
        self.select("a:{x:{y:1}} / 1", 'a.x.y')

        self.select("a:{w.x.y:1} + 1", 'a.w.x.y')
        self.select("a:{w:{x:{y:1}}} + 1", 'a.w.x.y')

if __name__ == '__main__':
    mldb.run_tests()
