#
# MLDB-1705-function-application-path.py
# 6 juin 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest, json

mldb2 = mldb
mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1705(MldbUnitTest):
    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "data1", "type": "sparse.mutable" })
        ds.record_row("a",[["x", 1, 0]])
        ds.commit()

        ds = mldb.create_dataset({ "id": "data2", "type": "sparse.mutable" })
        ds.record_row("a",[["y", 2, 0]])
        ds.commit()

        mldb.put("/v1/functions/func", {
            "type": "sql.expression",
            "params": {
                "expression": """
                    horizontal_string_agg({data1.x, data2.y}, '-')
                """
            }
        })

    def test_output_of_join(self):
        self.assertTableResultEquals(
            mldb.query("""
                select func({*}) as *
                from data1
                join data2 on data1.rowName() = data2.rowName()
            """),
            [
                [
                    "_rowName",
                    "\"horizontal_string_agg({data1.x, data2.y}, '-')\""
                ],
                [
                    "[a]-[a]",
                    "1-2"
                ]
            ])


    def test_calling_from_application(self):
        data = {
            "data1": { "x": 1 },
            "data2": { "y": 2 }
        }

        rez = mldb.get("/v1/functions/func/application",
                       input=json.dumps(data))
        js_rez = rez.json()

        self.assertEqual(
            js_rez["output"]["horizontal_string_agg({data1.x, data2.y}, '-')"],
            "1-2")

    def test_calling_from_application_with_body(self):
        data = {
            "data1": { "x": 1 },
            "data2": { "y": 2 }
        }

        rez = mldb.get("/v1/functions/func/application",
                       data={"input" : data}).json()
        self.assertEqual(
            rez["output"]["horizontal_string_agg({data1.x, data2.y}, '-')"],
            "1-2")

    def test_calling_from_application_double_params(self):
        data = {
            "data1": { "x": 1 },
            "data2": { "y": 2 }
        }

        msg = "You cannot mix query string and body parameters"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.get("/v1/functions/func/application",
                     input=json.dumps(data), data={"input" : data})

mldb.run_tests()
