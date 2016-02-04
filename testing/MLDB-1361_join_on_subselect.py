#
# MLDB-1361_join_on_subselect.py
# Francois Maillet, 2016-02-04
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest
import json

mldb = mldb_wrapper.wrap(mldb) # noqa

class SampleTest(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "text", "type": "sparse.mutable" })
        ds.record_row("a",[["txt", "raise shields", 0]])
        ds.record_row("b",[["txt", "set a course", 0]])
        ds.commit()
        
        ds = mldb.create_dataset({ "id": "sub1", "type": "sparse.mutable" })
        ds.record_row("row_a",[["warp", 8, 0]])
        ds.commit()
        
        ds = mldb.create_dataset({ "id": "sub2", "type": "sparse.mutable" })
        ds.record_row("row_b",[["warp", 9, 0]])
        ds.commit()

    def test_select_x_works(self):
        # mldb.get asserts the result status_code is >= 200 and < 400
        rez = mldb.get("/v1/query", q="""

            SELECT
                text.txt, sub1.warp, sub2.warp
            FROM text
            LEFT JOIN sub1 ON text.rowName() = regex_replace(sub1.rowName(), 'row_', '')
            LEFT JOIN sub2 ON text.rowName() = regex_replace(sub2.rowName(), 'row_', '')

        """)
        
        mldb.log(rez.json())
    
    def test_subselect_works(self):
        # mldb.get asserts the result status_code is >= 200 and < 400
        self.assertQueryResult(mldb.query("""

            SELECT
                text.txt, tbl1.warp, tbl2.warp
            FROM text
            LEFT JOIN (
                SELECT warp, regex_replace(rowName(), 'row_', '') as rowName
                FROM sub1
            ) as tbl1 ON text.rowName() = tbl1.rowName
            LEFT JOIN (
                SELECT warp, regex_replace(rowName(), 'row_', '') as rowName
                FROM sub2
            ) as tbl2 ON text.rowName() = tbl2.rowName
        """),
            [
                {
                    "rowName": "a-row_a-null",
                    "rowHash": "8711ec4b866a7c29",
                    "columns": [
                        [
                            "text.txt",
                            "raise shields",
                            "1970-01-01T00:00:00Z"
                        ],
                        [
                            "tbl1.warp",
                            8,
                            "1970-01-01T00:00:00Z"
                        ],
                        [
                            "tbl2.warp",
                            null,
                            "-Inf"
                        ]
                    ]
                },
                {
                    "rowName": "b-null-row_b",
                    "rowHash": "a4c2b6a8ca3bb829",
                    "columns": [
                        [
                            "text.txt",
                            "set a course",
                            "1970-01-01T00:00:00Z"
                        ],
                        [
                            "tbl1.warp",
                            null,
                            "-Inf"
                        ],
                        [
                            "tbl2.warp",
                            9,
                            "1970-01-01T00:00:00Z"
                        ]
                    ]
                }
            ])

mldb.run_tests()

