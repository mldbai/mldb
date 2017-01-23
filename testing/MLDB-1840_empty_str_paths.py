#
# MLDB-1840_empty_str_paths.py
# Francois Maillet, 2016-07-21
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1840EmptyStrPaths(MldbUnitTest):  # noqa

    def test_parse_empty_col_name(self):
        self.assertTableResultEquals(
            mldb.query("""
                SELECT parse_json('{"": 5, "pwet":10}') AS *
            """),
            [
                ["_rowName", '""', "pwet"],
                [  "result",  5, 10]
            ]
        )

    def test_select_star_subselect_with_empty_col_name(self):
        self.assertTableResultEquals(
            mldb.query("""
                SELECT * FROM (
                    SELECT parse_json('{"": 5, "pwet":10}') AS *
                )
            """),
            [
                ["_rowName", '""', "pwet"],
                [  "result",  5, 10]
            ]
        )

    def test_select_named_col_subselect_with_empty_col_name(self):
        self.assertTableResultEquals(
            mldb.query("""
                SELECT pwet FROM (
                    SELECT parse_json('{"": 5, "pwet":10}') AS *
                )
            """),
            [
                ["_rowName", "pwet"],
                [  "result", 10]
            ]
        )

    def test_select_empty_col_subselect_with_empty_col_name(self):
        self.assertTableResultEquals(
            mldb.query("""
                SELECT "" FROM (
                    SELECT parse_json('{"": 5, "pwet":10}') AS *
                )
            """),
            [
                ["_rowName", '""'],
                [  "result", 5]
            ]
        )

    def test_post_empty_col_name(self):
        mldb.put('/v1/datasets/empty_col_name', {'type' : 'sparse.mutable'})
        mldb.post('/v1/datasets/empty_col_name/rows',
                  {'rowName' : 'row', 'columns' : [['', 'val', 0]]})
        mldb.post('/v1/datasets/empty_col_name/commit')
        res = mldb.query("SELECT * FROM empty_col_name")
        self.assertTableResultEquals(res, [
            ['_rowName', '""'],
            ["row", "val"]
        ])

    def test_post_empty_row_name(self):
        mldb.put('/v1/datasets/empty_row_name', {'type' : 'sparse.mutable'})
        mldb.post('/v1/datasets/empty_row_name/rows',
                  {'rowName' : '', 'columns' : [['col', 'val', 0]]})
        mldb.post('/v1/datasets/empty_row_name/commit')
        res = mldb.query("SELECT * FROM empty_row_name")
        self.assertTableResultEquals(res, [
            ['_rowName', 'col'],
            ['""', "val"]
        ])


if __name__ == '__main__':
    mldb.run_tests()
