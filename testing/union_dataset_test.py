#
# union_dataset_test.py
# Francois-Michel L'Heureux, 2016-09-20
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class UnionDatasetTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds1', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['colA', 'A', 1]])
        ds.commit()

        ds = mldb.create_dataset({'id' : 'ds2', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['colB', 'B', 1]])
        ds.commit()

        ds = mldb.create_dataset({'id' : 'ds3', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['colA', 'AA', 1], ['colB', 'BB', 1]])
        ds.record_row('row2', [['colA', 'A', 1], ['colC', 'C', 1]])
        ds.commit()

    def test_dataset(self):
        mldb.put('/v1/datasets/union_ds', {
            'type' : 'union',
            'params' : {
                'datasets' : [{'id' : 'ds1'}, {'id' : 'ds2'}]
            }
        })

        res = mldb.query("SELECT colA, colB FROM union_ds ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB'],
            ['0.row1', 'A', None],
            ['1.row1', None, 'B']
        ])

        res = mldb.query("SELECT * FROM union_ds ORDER BY rowName() LIMIT 1")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA'],
            ['0.row1', 'A']
        ])

        res = mldb.query("SELECT * FROM union_ds ORDER BY rowName() OFFSET 1")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colB'],
            ['1.row1', 'B']
        ])

        mldb.put('/v1/datasets/union_ds2', {
            'type' : 'union',
            'params' : {
                'datasets' : [{'id' : 'ds3'}, {'id' : 'ds3'}]
            }
        })
        res = mldb.query(
            "SELECT colA, colB, colC FROM union_ds2 ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB', 'colC'],
            ['0.row1', 'AA', 'BB', None],
            ['0.row2', 'A', None, 'C'],
            ['1.row1', 'AA', 'BB', None],
            ['1.row2', 'A', None, 'C']
        ])

    def test_query(self):
        res = mldb.query(
            "SELECT colA, colB FROM union(ds1, ds2) ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB'],
            ['0.row1', 'A', None],
            ['1.row1', None, 'B']
        ])

        res = mldb.query(
            "SELECT * FROM union(ds1, ds2) ORDER BY rowName() LIMIT 1")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA'],
            ['0.row1', 'A']
        ])

        res = mldb.query(
            "SELECT * FROM union(ds1, ds2) ORDER BY rowName() OFFSET 1")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colB'],
            ['1.row1', 'B']
        ])

        res = mldb.query(
            "SELECT colA, colB, colC FROM union(ds3, ds3) ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB', 'colC'],
            ['0.row1', 'AA', 'BB', None],
            ['0.row2', 'A', None, 'C'],
            ['1.row1', 'AA', 'BB', None],
            ['1.row2', 'A', None, 'C']
        ])

    def test_equivalent_query(self):
        res = mldb.query("""
            SELECT s1.* AS *, s2.* AS *
            FROM (SELECT * FROM ds1 ) AS s1
            OUTER JOIN (SELECT * FROM ds2) AS s2 ON false
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB'],
            ['[]-[row1]', None, 'B'],
            ['[row1]-[]', 'A', None]
        ])

    def test_query_function(self):
        mldb.put('/v1/functions/union_qry', {
            'type' : 'sql.query',
            'params' : {
                'query' : 'SELECT * FROM union(ds3, ds3) ORDER BY rowName()'
            }
        })

        res = mldb.get('/v1/functions/union_qry/application').json()
        self.assertEqual(res, {
            'output' : {
                'colB' : 'BB',
                'colA' : 'AA'
            }
        })

    def test_merge_over_union(self):
        res = mldb.query("""
            SELECT * FROM merge(union(ds1, ds2), ds3)
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "colA", "colB", "colC"],
            ["row1", "AA", "BB", None],
            ["row1", "AA", "BB", None],
            ["row1", "AA", "BB", None],
            ["row2", "A", None, "C"]
        ])

    def test_invalid_where(self):
        res = mldb.query("SELECT * FROM union(ds1, ds2) WHERE foo='bar'")
        self.assertEqual(len(res), 1)

    def test_where_index_out_of_range(self):
        res = mldb.query(
            "SELECT * FROM union(ds1, ds2) WHERE rowName() = '12.row1'")
        self.assertEqual(len(res), 1)

    def test_group_by(self):
        res = mldb.query("""
            SELECT colA FROM union(ds1, ds2) WHERE true GROUP BY colA
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "colA"],
            ["[null]", None],
            ["\"[\"\"A\"\"]\"", "A"]
        ])

if __name__ == '__main__':
    mldb.run_tests()
