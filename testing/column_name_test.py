#
# column_name_test.py
# Francois-Michel L Heureux, 2016-07-21
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class ColumnNameTest(MldbUnitTest):  # noqa

    def select(self, select, expected):
        res = mldb.query("SELECT {}".format(select))
        self.assertEqual(res[0][1], expected)

    def test_object_extrac_star(self):
        self.select("c* FROM (SELECT col.x:1)", 'col.x')
        #self.select("c* AS * FROM (SELECT col.x:1)", 'x')  # no longer works with structured subselects  
        self.select("* AS * FROM (SELECT col.x:1)", 'col.x')  

if __name__ == '__main__':
    mldb.run_tests()
