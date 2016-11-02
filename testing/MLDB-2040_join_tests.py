#
# MLDB-2040_join_tests.py
# Francois-Michel L'Heureux, 2016-11-02
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2040JoinTests(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'a', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['one', 1, 0], ['two', 1, 0]])
        ds.record_row('row2', [['one', 1, 0], ['two', 2, 0]])
        ds.record_row('row3', [['one', 2, 0], ['two', 1, 0]])
        ds.record_row('row4', [['one', 2, 0], ['two', 1, 0]])
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
            ["[row4]-[]", 2, 1]
        ])

    @unittest.expectedFailure
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
            ["[row4]-[]", 2, 1, None, None]
        ])

    @unittest.expectedFailure
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
            ["_rowName", "a.one", "a.two", "rhs.one", "rhs.two"],
            ["[row1]-[row11]", 1, 1, 1, 1],
            ["[row1]-[row1]", 1, 1, 1, 1],
            ["[row2]-[row22]", 1, 2, 1, 2],
            ["[row2]-[row2]", 1, 2, 1, 2],
            ["[row3]-[]", 2, 1, None, None],
            ["[row4]-[]", 2, 1, None, None]
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

    @unittest.expectedFailure
    def test_right_join_rsh(self):
        ds = mldb.create_dataset({'id' : 'rj_rhs', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['one', 1, 0], ['two', 1, 0]])
        ds.record_row('row2', [['one', 1, 0], ['two', 2, 0]])
        ds.commit()
        res = mldb.query("""
            SELECT * FROM a
            RIGTH JOIN rj_rhs ON a.one = rj_rhs.one AND a.two = rj_rhs.two
            ORDER BY rowName()""")
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "rhs.one", "rhs.two"],
            ["[row1]-[row1]", 1, 1, 1, 1],
            ["[row2]-[row2]", 1, 2, 1, 2]
        ])

    @unittest.expectedFailure
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
            RIGTH JOIN rj_rhs_multi_match
                ON a.one = rj_rhs_multi_match.one
                AND a.two = rj_rhs_multi_match.two
            ORDER BY rowName()""")
        self.assertTableResultEquals(res, [
            ["_rowName", "a.one", "a.two", "rhs.one", "rhs.two"],
            ["[row1]-[row11]", 1, 1, 1, 1],
            ["[row1]-[row1]", 1, 1, 1, 1],
            ["[row2]-[row22]", 1, 2, 1, 2],
            ["[row2]-[row2]", 1, 2, 1, 2]
        ])


if __name__ == '__main__':
    mldb.run_tests()
