#
# MLDB-2074-empty-join.py
# Francois-Michel L'Heureux, 2016-11-02
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2074JoinTests(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'a', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['one', 1, 0], ['two', 1, 0]])
        ds.record_row('row2', [['one', 1, 0], ['two', 2, 0]])
        ds.record_row('row3', [['one', 2, 0], ['two', 1, 0]])
        ds.record_row('row4', [['one', 2, 0], ['two', 2, 0]])
        ds.commit()

        ds = mldb.create_dataset({'id' : 'empty', 'type' : 'sparse.mutable'})
        ds.commit()


    def test_left_join_constant_where(self):        
        res = mldb.query("""
            SELECT * FROM a
            LEFT JOIN empty ON a.one = empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two"],
            ["[row1]-[]", 1, 1],
            ["[row2]-[]", 1, 2],
            ["[row3]-[]", 2, 1],
            ["[row4]-[]", 2, 2]
        ])

    def test_left_join_equi_pipeline(self):
        res = mldb.query("""
            SELECT * FROM a
            LEFT JOIN empty ON a.one = empty.one AND a.two = empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two"],
            ["[row1]-[]", 1, 1],
            ["[row2]-[]", 1, 2],
            ["[row3]-[]", 2, 1],
            ["[row4]-[]", 2, 2]
        ])

    def test_left_join_equi_pipeline_reverse(self):
        res = mldb.query("""
            SELECT * FROM empty
            LEFT JOIN a ON a.one = empty.one AND a.two = empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            [
                "_rowName"
            ]
        ])

    def test_right_join_constant_where(self):        
        res = mldb.query("""
            SELECT * FROM a
            RIGHT JOIN empty ON a.one = empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            [
                "_rowName"
            ]
        ])

    def test_right_join_equi_pipeline(self):
        res = mldb.query("""
            SELECT * FROM a
            RIGHT JOIN empty ON a.one = empty.one AND a.two = empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            [
                "_rowName"
            ]
        ])

    def test_right_join_equi_pipeline_reverse(self):
        res = mldb.query("""
            SELECT * FROM empty
            RIGHT JOIN a ON a.one = empty.one AND a.two = empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two"],
            ["[]-[row1]", 1, 1],
            ["[]-[row2]", 1, 2],
            ["[]-[row3]", 2, 1],
            ["[]-[row4]", 2, 2]
        ])

    def test_full_join_equi_pipeline(self):
        res = mldb.query("""
            SELECT * FROM a
            FULL JOIN empty ON a.one = empty.one AND a.two = empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two"],
            ["[row1]-[]", 1, 1],
            ["[row2]-[]", 1, 2],
            ["[row3]-[]", 2, 1],
            ["[row4]-[]", 2, 2]
        ])

    def test_full_join_equi_pipeline_reverse(self):
        res = mldb.query("""
            SELECT * FROM empty
            FULL JOIN a ON a.one = empty.one AND a.two = empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two"],
            ["[]-[row1]", 1, 1],
            ["[]-[row2]", 1, 2],
            ["[]-[row3]", 2, 1],
            ["[]-[row4]", 2, 2]
        ])

    def test_left_join_cross_pipeline(self):
        res = mldb.query("""
            SELECT * FROM a
            LEFT JOIN empty ON a.one <= empty.one AND a.two <= empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two"],
            ["[row1]-[]", 1, 1],
            ["[row2]-[]", 1, 2],
            ["[row3]-[]", 2, 1],
            ["[row4]-[]", 2, 2]
        ])

    def test_left_join_cross_pipeline_reverse(self):
        res = mldb.query("""
            SELECT * FROM empty
            LEFT JOIN a ON a.one <= empty.one AND a.two <= empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            [
                "_rowName"
            ]
        ])

    def test_right_join_cross_pipeline(self):
        res = mldb.query("""
            SELECT * FROM a
            RIGHT JOIN empty ON a.one <= empty.one AND a.two <= empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            [
                "_rowName"
            ]
        ])
    def test_right_join_cross_pipeline_reverse(self):
        res = mldb.query("""
            SELECT * FROM empty
            RIGHT JOIN a ON a.one <= empty.one AND a.two <= empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two"],
            ["[]-[row1]", 1, 1],
            ["[]-[row2]", 1, 2],
            ["[]-[row3]", 2, 1],
            ["[]-[row4]", 2, 2]
        ])

    def test_full_join_cross_pipeline(self):
        res = mldb.query("""
            SELECT * FROM a
            FULL JOIN empty ON a.one <= empty.one AND a.two <= empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two"],
            ["[row1]-[]", 1, 1],
            ["[row2]-[]", 1, 2],
            ["[row3]-[]", 2, 1],
            ["[row4]-[]", 2, 2]
        ])

    def test_full_join_cross_pipeline_reverse(self):
        res = mldb.query("""
            SELECT * FROM empty
            FULL JOIN a ON a.one <= empty.one AND a.two <= empty.one
            ORDER BY rowName()""")

        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two"],
            ["[]-[row1]", 1, 1],
            ["[]-[row2]", 1, 2],
            ["[]-[row3]", 2, 1],
            ["[]-[row4]", 2, 2]
        ])

if __name__ == '__main__':
    mldb.run_tests()
