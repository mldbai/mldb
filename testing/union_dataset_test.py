#
# union_dataset_test.py
# Francois-Michel L'Heureux, 2016-09-20
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
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

        ds = mldb.create_dataset({'id' : 'ds4', 'type' : 'sparse.mutable'})
        for num in range(1,1000):
            ds.record_row('row' + str(num), [['colA', 'AA', 1]])
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

    def test_dataset_stream(self):
        mldb.put('/v1/datasets/union_ds', {
            'type' : 'union',
            'params' : {
                'datasets' : [{'id' : 'ds1'}, {'id' : 'ds4'}]
            }
        })

        res = mldb.query("SELECT colA, count(*) FROM union_ds GROUP BY colA")
        self.assertTableResultEquals(res, [
            [   "_rowName", "colA", "count(*)" ],
            [   "\"[\"\"A\"\"]\"", "A", 1 ],
            [   "\"[\"\"AA\"\"]\"", "AA", 999]])

    @unittest.skip("Unimplemented support")
    def test_query_from_ds(self):
        res = mldb.query("""
            SELECT colA FROM ds1
            UNION
            SELECT colB FROM ds2
        """)
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB'],
            ['0.row1', 'A', None],
            ['1.row1', None, 'B']
        ])

    @unittest.skip("Unimplemented support")
    def test_query_w_and_wo_ds(self):
        res = mldb.query("""
            SELECT 1
            UNION
            SELECT colB FROM ds2
        """)
        self.assertTableResultEquals(res, [
            ['_rowName', '1', 'colB'],
            ['0.result', 1, None],
            ['1.row1', None, 'B']
        ])

        res = mldb.query("""
            SELECT colB FROM ds2
            UNION
            SELECT 1
        """)
        self.assertTableResultEquals(res, [
            ['_rowName', 'colB', '1'],
            ['0.row1', 'B', None],
            ['1.result', None, 1]
        ])

    @unittest.skip("Unimplemented support")
    def test_query_wo_ds(self):
        res = mldb.query("""
            SELECT 1
            UNION
            SELECT 2
            UNION
            SELECT 1, 2
            UNION
            SELECT 3
        """)
        self.assertTableResultEquals(res, [
            ['_rowName', '1', '2', '3'],
            ['0.result', 1, None, None],
            ['1.result', None, 2, None],
            ['2.result', 1, 2, None],
            ['3.result', None, None, 3]
        ])

    @unittest.skip("Unimplemented support")
    def test_query_wo_ds_nested(self):
        res = mldb.query("""
            SELECT * FROM (
                SELECT 1
                UNION
                SELECT 2
            )
        """)
        self.assertTableResultEquals(res, [
            ['_rowName', '1', '2'],
            ['0.result', 1, None],
            ['1.result', None, 2]
        ])

    @unittest.skip("Unimplemented support")
    def test_query_wo_ds_nested_over_union(self):
        res = mldb.query("""
            SELECT * FROM (
                SELECT 1
                UNION
                SELECT 2
            )
            UNION
            SELECT 3
        """)
        self.assertTableResultEquals(res, [
            ['_rowName', '1', '2', '3'],
            ['0.0.result', 1, None, None],
            ['0.1.result', None, 2, None],
            ['1.result', None, None, 3]
        ])

    @unittest.skip("Unimplemented support")
    def test_query_over_union(self):
        res = mldb.query("""
            SELECT * FROM (
                SELECT 1
                UNION
                SELECT 2
            )
            WHERE rowName() = '0.result'
        """)
        self.assertTableResultEquals(res, [
            ['_rowName', '1'],
            ['0.result', 1]
        ])

        res = mldb.query("""
            SELECT count({*}) AS * FROM (
                SELECT 1, 2, 3
                UNION
                SELECT 2, 1
                UNION
                SELECT 1
            )
        """)
        self.assertTableResultEquals(res, [
            ['_rowName', '1', '2', '3'],
            ['[]', 3, 2, 1]
        ])

    def test_where(self):
        mldb.put('/v1/datasets/union_test_where', {
            'type' : 'union',
            'params' : {
                'datasets' : [{'id' : 'ds1'}, {'id' : 'ds2'}]
            }
        })

        res = mldb.query("SELECT * FROM union_test_where WHERE colA='123'")
        self.assertEquals(len(res), 1)


if __name__ == '__main__':
    mldb.run_tests()
