#
# MLDB-1172_column_expr_fail.py
# Datacratic, 2015
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class ColumnExprTest(MldbUnitTest):  # noqa

    def test_base(self):
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'toy'
        }

        dataset = mldb.create_dataset(dataset_config)

        dataset.record_row("rowA", [["feat1", 1, 0],
                                    ["feat2", 1, 0],
                                    ["feat3", 1, 0]])
        dataset.record_row("rowB", [["feat1", 1, 0],
                                    ["feat2", 1, 0]]),
        dataset.record_row("rowC", [["feat1", 1, 0]])
        dataset.commit()

        mldb.get(
            "/v1/query",
            q="select COLUMN EXPR (ORDER BY rowCount() DESC LIMIT 2) from toy")

        mldb.get(
            "/v1/query",
            q="""SELECT COLUMN EXPR (
                    WHERE regex_match(columnName(), 'feat[[:digit:]]')
                 ORDER BY rowCount() DESC LIMIT 2) from toy""")

    def test_column_expr_in_where(self):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('user1', [['0:behA', 1, 0]])
        ds.record_row('user2', [['0:behB', 1, 0]])
        ds.commit()

        # with defined column name it works
        res = mldb.query('SELECT * FROM ds '
                         'WHERE horizontal_sum({"0:behA"}) > 0')
        self.assertTableResultEquals(res, [
            ["_rowName", "0:behA"],
            ["user1", 1]
        ])

        # with column expression, the maths work
        res = mldb.query("""
            SELECT horizontal_sum({COLUMN EXPR (
                WHERE regex_match(columnName(), '[[:digit:]]+:behA'))}) AS res
            FROM ds
            ORDER BY rowName()
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "res"],
            ["user1", 1],
            ["user2", 0],
        ])

        # but using it as a where condition fails
        res = mldb.query("""
            SELECT * FROM ds
            WHERE horizontal_sum({COLUMN EXPR (
                WHERE regex_match(columnName(), '[[:digit:]]+:behA'))}) > 0
        """)
        self.assertTableResultEquals(res, [
            ["_rowName", "0:behA"],
            ["user1", 1]
        ])



if __name__ == '__main__':
    mldb.run_tests()
