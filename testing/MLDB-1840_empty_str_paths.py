#
# MLDB-1840_empty_str_paths.py
# Francois Maillet, 2016-07-21
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1840EmptyStrPaths(MldbUnitTest):  # noqa

    def test_it(self):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row', [['', 'val', 0]])
        ds.commit()
        self.assertTableResultEquals(
            mldb.query("""
                SELECT parse_json('{"": 5, "pwet":10}') AS *
            """),
            [
                ["_rowName", "", "pwet"],
                [  "result",  5, 10]
            ]
        )

        self.assertTableResultEquals(
            mldb.query("""
                SELECT parse_json('{"": 5, "pwet":10}') AS *
                WHERE "" != 5
            """),
            [
                ["_rowName"]
            ]
        )

        self.assertTableResultEquals(
            mldb.query("""
                SELECT * FROM (
                    SELECT parse_json('{"": 5, "pwet":10}') AS *
                )
            """),
            [
                ["_rowName", "", "pwet"]
                [  "result",  5, 10]
            ]
        )

        self.assertTableResultEquals(
            mldb.query("""
                SELECT pwet FROM (
                    SELECT parse_json('{"": 5, "pwet":10}') AS *
                )
            """),
            [
                ["_rowName", "pwet"]
                [  "result", 10]
            ]
        )

        self.assertTableResultEquals(
            mldb.query("""
                SELECT "" FROM (
                    SELECT parse_json('{"": 5, "pwet":10}') AS *
                )
            """),
            [
                ["_rowName", ""]
                [  "result", 5]
            ]
        )



if __name__ == '__main__':
    mldb.run_tests()
