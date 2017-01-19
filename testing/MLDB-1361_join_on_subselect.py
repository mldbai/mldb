#
# MLDB-1361_join_on_subselect.py
# Francois Maillet, 2016-02-04
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
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

        expected = [
             {
                 "rowName": "[a]-[row_a]-[]", 
                 "columns": [
                     [
                         "sub1.warp", 
                         8, 
                         "1970-01-01T00:00:00Z"
                     ], 
                     [
                         "sub2.warp", 
                         None, 
                         "-Inf"
                     ], 
                     [
                         "text.txt", 
                         "raise shields", 
                         "1970-01-01T00:00:00Z"
                     ]
                 ]
             }, 
             {
                 "rowName": "[b]-[]-[row_b]", 
                 "columns": [
                     [
                         "sub1.warp", 
                         None, 
                         "-Inf"
                     ], 
                     [
                         "sub2.warp", 
                         9, 
                         "1970-01-01T00:00:00Z"
                     ], 
                     [
                         "text.txt", 
                         "set a course", 
                         "1970-01-01T00:00:00Z"
                     ]
                 ]
             }
        ]
        assert rez.json() == expected
    
    def test_subselect_works(self):

        rez = mldb.get("/v1/query", q="""
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
        """)


        mldb.log(rez.json())

        expected = [
            {
                "rowName": "[a]-[row_a]-[]",
                "columns": [
                    [
                        "tbl1.warp",
                        8,
                        "1970-01-01T00:00:00Z"
                    ],
                    [
                        "tbl2.warp",
                        None,
                        "-Inf"
                    ],
                    [
                        "text.txt",
                        "raise shields",
                        "1970-01-01T00:00:00Z"
                    ]
                ]
            },
            {
                "rowName": "[b]-[]-[row_b]",
                "columns": [
                    [
                        "tbl1.warp",
                        None,
                        "-Inf"
                    ],
                    [
                        "tbl2.warp",
                        9,
                        "1970-01-01T00:00:00Z"
                    ],
                    [
                        "text.txt",
                        "set a course",
                        "1970-01-01T00:00:00Z"
                    ],
                ]
            }
        ]

        assert rez.json() == expected
       

mldb.run_tests()

