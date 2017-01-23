#
# MLDB-1707-no-context-resolve-table.py
# Mathieu Marquis Bolduc, 2016-06-07
# Copyright (c) 2016 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1707Test(MldbUnitTest):  # noqa

    def test_single_val(self):
        conf = {
            "type": "sql.expression",
            "params": {
                "expression": "input.*"
            }
        }
        mldb.put("/v1/functions/f1", conf)

        rez = mldb.get("/v1/query", q="select f1( {input: {xx: 1, xy: 2}} ) as *")
        js_rez = rez.json()
        mldb.log(js_rez)
        expected = [
        {
            "rowName": "result",
            "columns": [
                [
                    "input.xx",
                    1,
                    "-Inf"
                ],
                [
                    "input.xy",
                    2,
                    "-Inf"
                ]
            ]
        }
        ]
        self.assertEqual(expected, js_rez)

if __name__ == '__main__':
    mldb.run_tests()
