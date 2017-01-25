#
# MLDB-2111-group-by-expression.py
# Mahtieu Marquis Bolduc, 2017-01-24
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2111GroupByTests(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('0', [['x', 1, 0], ['y', 1, 0]])
        ds.record_row('1', [['x', 1, 0], ['y', 2, 0]])
        ds.record_row('2', [['x', 2, 0], ['y', 1, 0]])
        ds.record_row('3', [['x', 2, 0], ['y', 2, 0]])
        ds.commit()

    def test_groupby_expression(self):
        res = mldb.query("""
            SELECT x+1 FROM (SELECT x:1) GROUP BY x+1
        """)

        self.assertTableResultEquals(res, 
            [[ "_rowName", "x+1"],
             [ "[2]", 2 ]])

    def test_groupby_expression_named(self):
        res = mldb.query("""
            SELECT x+1 as z FROM (SELECT x:1) GROUP BY x+1
        """)

        self.assertTableResultEquals(res, 
            [[ "_rowName", "z"],
             [ "[2]", 2 ]])

    def test_groupby_sub_expression(self):
        res = mldb.query("""
            SELECT (x+1)*3 as z FROM (SELECT x:1) GROUP BY x+1
        """)

        self.assertTableResultEquals(res, 
            [[ "_rowName", "z"],
             [ "[2]", 6 ]])

    def test_groupby_expression_multiple_key(self):
        res = mldb.query("""
            SELECT x+1 FROM ds GROUP BY x+1, y*2
        """)

        self.assertTableResultEquals(res, 
            [[ "_rowName", "x+1"],
             [ "[2,2]", 2 ],
             [ "[2,4]", 2 ],
             [ "[3,2]", 3 ],
             [ "[3,4]", 3 ]])

    ## Rowname & rowHash in the select is different than in the group by
    def test_groupby_rowname(self):
        res = mldb.query("""
            SELECT rowName() FROM ds GROUP BY rowName() LIMIT 1
        """)

        self.assertTableResultEquals(res, 
            [["_rowName", "rowName()"],
             ["\"[\"\"0\"\"]\"","\"[\"\"0\"\"]\""]])

        res = mldb.query("""
            SELECT rowHash() FROM ds GROUP BY rowHash() LIMIT 1
        """)

        self.assertTableResultEquals(res, 
            [["_rowName","rowHash()"],
             ["[10408321403207385874]", 11729733417614312054]])

    def test_groupby_argument(self):
        res = mldb.query("""
            SELECT sqrt(x * 3) as z FROM ds GROUP BY x * 3
        """)

        self.assertTableResultEquals(res, 
            [["_rowName", "z"],
             ["[3]",1.7320508075688772],
             ["[6]",2.449489742783178]])

    def test_groupby_expression_function(self):
        res = mldb.query("""
            SELECT horizontal_sum({x,y}) + 1 as z FROM ds GROUP BY horizontal_sum({x,y})
        """)

        self.assertTableResultEquals(res, 
            [["_rowName", "z"],
             ["[2]",3],
             ["[3]",4],
             ["[4]",5]])

    def test_groupby_named(self):
        res = mldb.query("""
            SELECT x+1  NAMED (x+1)*2 FROM (SELECT x:1) GROUP BY x+1
        """)

        self.assertTableResultEquals(res, 
            [[ "_rowName", "x+1"],
             [ "4", 2 ]])

    def test_groupby_orderby(self):
        res = mldb.query("""
            SELECT x+1  FROM ds GROUP BY x+1 ORDER BY x+1
        """)

        self.assertTableResultEquals(res, 
            [["_rowName", "x+1"],
             ["[2]", 2],
             ["[3]", 3]])

    def test_groupby_having(self):
        res = mldb.query("""
            SELECT 0 as z FROM ds GROUP BY x+1 HAVING x+1 = 3
        """)

        self.assertTableResultEquals(res, 
            [["_rowName","z"],
             ["[3]", 0]])

if __name__ == '__main__':
    mldb.run_tests()