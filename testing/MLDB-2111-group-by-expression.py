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
        ds.record_row('0', [['x', 1, 1], ['y', 1, 2]])
        ds.record_row('1', [['x', 1, 3], ['y', 2, 4]])
        ds.record_row('2', [['x', 2, 5], ['y', 1, 6]])
        ds.record_row('3', [['x', 2, 7], ['y', 2, 8]])
        ds.commit()

        ds2 = mldb.create_dataset({'id' : 'ds2', 'type' : 'sparse.mutable'})
        ds2.record_row('0', [['x', 1, 1], ['x', 2, 2]])
        ds2.commit()

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
            SELECT rowName() FROM ds GROUP BY rowName()
        """)

        self.assertTableResultEquals(res, 
            [["_rowName", "rowName()"],
             ["\"[\"\"0\"\"]\"", "\"[\"\"0\"\"]\""],
             ["\"[\"\"1\"\"]\"", "\"[\"\"1\"\"]\""],
             ["\"[\"\"2\"\"]\"", "\"[\"\"2\"\"]\""],
             ["\"[\"\"3\"\"]\"", "\"[\"\"3\"\"]\""]])

        res = mldb.query("""
            SELECT rowHash() FROM ds GROUP BY rowHash()
        """)

        self.assertTableResultEquals(res, 
            [["_rowName","rowHash()"],
             ["[10408321403207385874]",11729733417614312054],
             ["[11275350073939794026]",9399015998024610411],
             ["[11413460447292444913]",4531258406341702386],
             ["[17472595041006102391]",12806200029519745032]])

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

    def test_groupby_temporal(self):
        res = mldb.query("""
            select temporal_latest({x}) as * from ds2 GROUP BY x
        """)

        self.assertTableResultEquals(res, 
            [["_rowName","x"],
             ["[2]", 2]])

        res = mldb.query("""
            select temporal_latest({x}) from ds2 group by temporal_latest({x})
        """)

        self.assertTableResultEquals(res, 
            [["_rowName","temporal_latest({x}).x"],
             ["\"[[[\"\"x\"\",[2,\"\"1970-01-01T00:00:02Z\"\"]]]]\"", 2]])

    def test_groupby_inexact(self):

        msg = "variable 'x' must appear in the GROUP BY clause or be used in an aggregate function"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            res = mldb.query("""
                SELECT x+1 FROM (SELECT x:1) GROUP BY 1+x
            """)

        msg = "variable 'x' must appear in the GROUP BY clause or be used in an aggregate function"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            res = mldb.query("""
                SELECT x+1*3 FROM (SELECT x:1) GROUP BY x+1
            """)     

if __name__ == '__main__':
    mldb.run_tests()