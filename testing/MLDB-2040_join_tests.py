#
# MLDB-2040_join_tests.py
# Francois-Michel L'Heureux, 2016-11-02
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2040JoinTests(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'a', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['one', 1, 0], ['two', 1, 0]])
        ds.record_row('row2', [['one', 1, 0], ['two', 2, 0]])
        ds.record_row('row3', [['one', 2, 0], ['two', 1, 0]])
        ds.record_row('row4', [['one', 2, 0], ['two', 2, 0]])
        ds.commit()

        ds = mldb.create_dataset({'id' : 'b', 'type' : 'sparse.mutable'})
        ds.record_row('row0', [['one', 0, 0]])
        ds.record_row('row1', [['one', 1, 0]])
        ds.record_row('row2', [['one', 2, 0]])
        ds.commit()


    def test_left_join_no_rhs(self):
        ds = mldb.create_dataset({'id' : 'no_rhs', 'type' : 'sparse.mutable'})
        ds.commit()
        res = mldb.query("""
            SELECT * FROM a
            LEFT JOIN no_rhs ON a.one = no_rhs.one
            ORDER BY rowName()""")
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two"],
            ["[row1]-[]", 1, 1],
            ["[row2]-[]", 1, 2],
            ["[row3]-[]", 2, 1],
            ["[row4]-[]", 2, 2]
        ])

    def test_left_join_rsh(self):
        ds = mldb.create_dataset({'id' : 'rhs', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['one', 1, 0], ['two', 1, 0]])
        ds.record_row('row2', [['one', 1, 0], ['two', 2, 0]])
        ds.commit()
        res = mldb.query("""
            SELECT * FROM a
            LEFT JOIN rhs ON a.one = rhs.one AND a.two = rhs.two
            ORDER BY rowName()""")
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "rhs.one", "rhs.two"],
            ["[row1]-[row1]", 1, 1, 1, 1],
            ["[row2]-[row2]", 1, 2, 1, 2],
            ["[row3]-[]", 2, 1, None, None],
            ["[row4]-[]", 2, 2, None, None]
        ])

    def test_left_join_rsh_multi_match(self):
        ds = mldb.create_dataset({
            'id' : 'rhs_multi_match',
            'type' : 'sparse.mutable'
        })
        ds.record_row('row1', [['one', 1, 0], ['two', 1, 0]])
        ds.record_row('row2', [['one', 1, 0], ['two', 2, 0]])
        ds.record_row('row22', [['one', 1, 0], ['two', 2, 0]])
        ds.record_row('row11', [['one', 1, 0], ['two', 1, 0]])
        ds.commit()
        res = mldb.query("""
            SELECT * FROM a
            LEFT JOIN rhs_multi_match
                ON a.one = rhs_multi_match.one AND a.two = rhs_multi_match.two
            ORDER BY rowName()""")
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "rhs_multi_match.one", "rhs_multi_match.two"],
            ["[row1]-[row11]", 1, 1, 1, 1],
            ["[row1]-[row1]", 1, 1, 1, 1],
            ["[row2]-[row22]", 1, 2, 1, 2],
            ["[row2]-[row2]", 1, 2, 1, 2],
            ["[row3]-[]", 2, 1, None, None],
            ["[row4]-[]", 2, 2, None, None]
        ])

    def test_right_join_no_rhs(self):
        ds = mldb.create_dataset({
            'id' : 'rj_no_rhs',
            'type' : 'sparse.mutable'
        })
        ds.commit()
        res = mldb.query("""
            SELECT * FROM a
            RIGHT JOIN rj_no_rhs ON a.one = rj_no_rhs.one
            ORDER BY rowName()""")
        self.assertTableResultEquals(res, [
            ["_rowName"]
        ])

    def test_right_join_rsh(self):
        ds = mldb.create_dataset({'id' : 'rj_rhs', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['one', 1, 0], ['two', 1, 0]])
        ds.record_row('row2', [['one', 1, 0], ['two', 2, 0]])
        ds.commit()
        res = mldb.query("""
            SELECT * FROM a
            RIGHT JOIN rj_rhs ON a.one = rj_rhs.one AND a.two = rj_rhs.two
            ORDER BY rowName()""")
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "rj_rhs.one", "rj_rhs.two"],
            ["[row1]-[row1]", 1, 1, 1, 1],
            ["[row2]-[row2]", 1, 2, 1, 2]
        ])

    def test_right_join_rsh_multi_match(self):
        ds = mldb.create_dataset({
            'id' : 'rj_rhs_multi_match',
            'type' : 'sparse.mutable'
        })
        ds.record_row('row1', [['one', 1, 0], ['two', 1, 0]])
        ds.record_row('row2', [['one', 1, 0], ['two', 2, 0]])
        ds.record_row('row22', [['one', 1, 0], ['two', 2, 0]])
        ds.record_row('row11', [['one', 1, 0], ['two', 1, 0]])
        ds.commit()
        res = mldb.query("""
            SELECT * FROM a
            RIGHT JOIN rj_rhs_multi_match
                ON a.one = rj_rhs_multi_match.one
                AND a.two = rj_rhs_multi_match.two
            ORDER BY rowName()""")
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "rj_rhs_multi_match.one", "rj_rhs_multi_match.two"],
            ["[row1]-[row11]", 1, 1, 1, 1],
            ["[row1]-[row1]", 1, 1, 1, 1],
            ["[row2]-[row22]", 1, 2, 1, 2],
            ["[row2]-[row2]", 1, 2, 1, 2]
        ])

    def test_left_join_gt(self):
        res = mldb.query("""
            SELECT * FROM a LEFT JOIN b ON a.one > b.one
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "b.one"],
            ["[row1]-[row0]", 1, 1, 0],
            ["[row2]-[row0]", 1, 2, 0],
            ["[row3]-[row0]", 2, 1, 0],
            ["[row3]-[row1]", 2, 1, 1],
            ["[row4]-[row0]", 2, 2, 0],
            ["[row4]-[row1]", 2, 2, 1]
        ])

    def test_left_join_gte(self):
        res = mldb.query("""
            SELECT * FROM a LEFT JOIN b ON a.one >= b.one
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "b.one"],
            ["[row1]-[row0]", 1, 1, 0],
            ["[row1]-[row1]", 1, 1, 1],
            ["[row2]-[row0]", 1, 2, 0],
            ["[row2]-[row1]", 1, 2, 1],
            ["[row3]-[row0]", 2, 1, 0],
            ["[row3]-[row1]", 2, 1, 1],
            ["[row3]-[row2]", 2, 1, 2],
            ["[row4]-[row0]", 2, 2, 0],
            ["[row4]-[row1]", 2, 2, 1],
            ["[row4]-[row2]", 2, 2, 2]
        ])

    def test_left_join_lt(self):
        res = mldb.query("""
            SELECT * FROM a LEFT JOIN b ON a.one < b.one
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "b.one"],
            ["[row1]-[row2]", 1, 1, 2],
            ["[row2]-[row2]", 1, 2, 2],
            ["[row3]-[]",2,1,None],
            ["[row4]-[]",2,2,None]
        ])

    def test_left_join_lte(self):
        res = mldb.query("""
            SELECT * FROM a LEFT JOIN b ON a.one <= b.one
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "b.one"],
            ["[row1]-[row1]", 1, 1, 1],
            ["[row1]-[row2]", 1, 1, 2],
            ["[row2]-[row1]", 1, 2, 1],
            ["[row2]-[row2]", 1, 2, 2],
            ["[row3]-[row2]", 2, 1, 2],
            ["[row4]-[row2]", 2, 2, 2]
        ])

    def test_left_join_no_match(self):
        res = mldb.query("""
            SELECT * FROM a LEFT JOIN b ON a.one - 100 > b.one
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two"],
            ["[row1]-[]", 1, 1],
            ["[row2]-[]", 1, 2],
            ["[row3]-[]", 2, 1],
            ["[row4]-[]", 2, 2]
        ])

    def test_left_join_gt_dual(self):
        res = mldb.query("""
            SELECT * FROM a LEFT JOIN b ON a.one > b.one AND a.two > b.one
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "b.one"],
            ["[row1]-[row0]", 1, 1, 0],
            ["[row2]-[row0]", 1, 2, 0],
            ["[row3]-[row0]", 2, 1, 0],
            ["[row4]-[row0]", 2, 2, 0],
            ["[row4]-[row1]", 2, 2, 1]
        ])

    def test_left_join_lt_with_op(self):
        res = mldb.query("""
            SELECT * FROM a LEFT JOIN b ON a.one > b.one AND a.two - 1 <  b.one
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "b.one"],
            ["[row1]-[]", 1, 1, None],
            ["[row2]-[]", 1, 2, None],
            ["[row3]-[row1]", 2, 1, 1],
            ["[row4]-[]", 2, 2, None]
        ])

    def test_left_join_gte_dual(self):
        res = mldb.query("""
            SELECT * FROM a LEFT JOIN b ON a.one >= b.one AND a.two >= b.one
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "b.one"],
            ["[row1]-[row0]", 1, 1, 0],
            ["[row1]-[row1]", 1, 1, 1],
            ["[row2]-[row0]", 1, 2, 0],
            ["[row2]-[row1]", 1, 2, 1],
            ["[row3]-[row0]", 2, 1, 0],
            ["[row3]-[row1]", 2, 1, 1],
            ["[row4]-[row0]", 2, 2, 0],
            ["[row4]-[row1]", 2, 2, 1],
            ["[row4]-[row2]", 2, 2, 2]
        ])

    def test_left_join_mix_dual(self):
        res = mldb.query("""
            SELECT * FROM a LEFT JOIN b ON a.one >= b.one AND a.two <= b.one
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "b.one"],
            ["[row1]-[row1]", 1, 1, 1],
            ["[row2]-[]", 1, 2, None],
            ["[row3]-[row1]", 2, 1, 1],
            ["[row3]-[row2]", 2, 1, 2],
            ["[row4]-[row2]", 2, 2, 2]
        ])

    def test_left_join_lt_dual(self):
        res = mldb.query("""
            SELECT * FROM a LEFT JOIN b ON a.one < b.one AND a.two < b.one
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "b.one"],
            ["[row1]-[row2]", 1, 1, 2],
            ["[row2]-[]", 1, 2, None],
            ["[row3]-[]", 2, 1, None],
            ["[row4]-[]", 2, 2, None]
        ])

    def test_left_join_lte_dual(self):
        res = mldb.query("""
            SELECT * FROM a LEFT JOIN b ON a.one <= b.one AND a.two <= b.one
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "b.one"],
            ["[row1]-[row1]", 1, 1, 1],
            ["[row1]-[row2]", 1, 1, 2],
            ["[row2]-[row2]", 1, 2, 2],
            ["[row3]-[row2]", 2, 1, 2],
            ["[row4]-[row2]", 2, 2, 2]
        ])

    def test_cross_full_nothing(self):

        ds = mldb.create_dataset({'id' : 'cross_rhs', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['one', 1, 0], ['two', 9, 0]])
        ds.record_row('row2', [['one', 1, 0], ['two', 9, 0]])
        ds.commit()

        res = mldb.query("""
            SELECT * FROM b FULL JOIN cross_rhs ON b.one < cross_rhs.one AND cross_rhs.two < b.one
            ORDER BY rowName()
        """)

        self.assertTableResultEquals(res, 
            [["_rowName", "cross_rhs.one", "cross_rhs.two", "b.one"],
             ["[]-[row1]", 1, 9, None],
             ["[]-[row2]", 1, 9, None],
             ["[row0]-[]", None, None, 0],
             ["[row1]-[]", None, None, 1],
             ["[row2]-[]", None, None, 2]
        ])

    def test_cross_full_something(self):

        ds = mldb.create_dataset({'id' : 'cross_rhs_2', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['one', 1, 0], ['two', 0, 0]])
        ds.record_row('row2', [['one', 1, 0], ['two', 1, 0]])
        ds.commit()

        res = mldb.query("""
            SELECT * FROM b FULL JOIN cross_rhs_2 ON b.one > cross_rhs_2.one AND b.one > cross_rhs_2.two
            ORDER BY rowName()
        """)

        self.assertTableResultEquals(res, 
            [["_rowName", "b.one", "cross_rhs_2.one", "cross_rhs_2.two"],
             ["[row0]-[]", 0, None, None],
             ["[row1]-[]", 1, None, None],
             ["[row2]-[row1]", 2, 1, 0],
             ["[row2]-[row2]", 2, 1, 1]
        ])

if __name__ == '__main__':
    mldb.run_tests()
