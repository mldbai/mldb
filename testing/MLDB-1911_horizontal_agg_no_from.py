#
# MLDB-1911_horizontal_agg_no_from.py
# Francois-Michel L'Heureux, 2016-09-30
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1911HorizontalAggNoFrom(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        # useless dataset to compare results with a FROM clause
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['A', 1, 0]])
        ds.commit()

    @unittest.expectedFailure
    def test_sum(self):
        expected = [
            ['_rowName', 'horizontal_sum({1 AS a, 2 AS b})'],
            ['row1', 3]
        ]

        # works
        res = mldb.query("SELECT horizontal_sum({1 AS a, 2 AS b}) FROM ds")
        self.assertTableResultEquals(res, expected)

        # fails
        res = mldb.query("SELECT horizontal_sum({1 AS a, 2 AS b})")
        self.assertTableResultEquals(res, expected)

    @unittest.expectedFailure
    def test_count(self):
        expected = [
            ['_rowName', 'horizontal_count({1 AS a, 2 AS b})'],
            ['row1', 2]
        ]

        # works
        res = mldb.query("SELECT horizontal_count({1 AS a, 2 AS b}) FROM ds")
        self.assertTableResultEquals(res, expected)

        # fails
        res = mldb.query("SELECT horizontal_count({1 AS a, 2 AS b})")
        self.assertTableResultEquals(res, expected)

    @unittest.expectedFailure
    def test_min(self):
        expected = [
            ['_rowName', 'horizontal_min({1 AS a, 2 AS b})'],
            ['row1', 1]
        ]

        # works
        res = mldb.query("SELECT horizontal_min({1 AS a, 2 AS b}) FROM ds")
        self.assertTableResultEquals(res, expected)

        # fails
        res = mldb.query("SELECT horizontal_min({1 AS a, 2 AS b})")
        self.assertTableResultEquals(res, expected)

    # TODO Add tests for the other horizontal aggregators.

if __name__ == '__main__':
    mldb.run_tests()
