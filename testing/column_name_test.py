#
# column_name_test.py
# Francois-Michel L Heureux, 2016-07-21
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class ColumnNameTest(MldbUnitTest):  # noqa

    def select(self, col, expected):
        res = mldb.query("SELECT {}".format(col))
        self.assertEqual(res[0][1], expected)

    def test_int(self):
        self.select(1, '1')

    def test_str(self):
        self.select("'patate'", "'patate'")

    def test_int_arith(self):
        self.select('1 + 10', '1 + 10')

    def test_float_arith(self):
        self.select('1 + 1.1', '"1 + 1.1"')

    def test_col_arith(self):
        self.select('a + b FROM (SELECT a:1, b:2)', 'a + b')

    @unittest.expectedFailure
    def test_obj_arith(self):
        self.select('a:{x:1} + 1', 'a:{x:1} + 1.x')  # currently returns a.x

    def test_as(self):
        self.select('x:1', 'x')
        self.select('1 AS x', 'x')

    def test_dotted_as(self):
        self.select('1 as a.b', 'a.b')

    def test_object(self):
        self.select("{x:1}", '{x:1}.x')

    def test_object_as_star(self):
        self.select("{x:1} AS *", 'x')

    def test_object_as_x(self):
        self.select("{x:1} AS x", 'x.x')

    def test_object_over_object_arith(self):
        self.select("{x:1} + {x:1}", '{x:1} + {x:1}.x')  # <--- ugly

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

#     def test_object_arith(self):
#         self.select("x.y:1 + 1", 'x.y')
#         self.select("x:{y:1} + 1", 'x.y')
#         self.select("{x.y:1 + 1}", '"{x.y:1 + 1}".x.y')
#         self.select("{x:{y:1} + 1}", '"{x.y:1 + 1}".x.y')
#         self.select("{x.y:1} + 1", '"{x.y:1} + 1".y')  # <--- x took a break?

if __name__ == '__main__':
    mldb.run_tests()
