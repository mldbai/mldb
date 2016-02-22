#
# MLDB-1266-import_json.py
# 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest
from functools import partial

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


class ImportJsonTest(unittest.TestCase):

    def assert_val(self, res, rowName, colName, value):
        for row in res:
            if str(row["rowName"]) != rowName:
                continue

            for col in row["columns"]:
                if col[0] == colName:
                    self.assertEqual(col[1], value)
                    return

            # did not find col
            mldb.log(res)
            mldb.log(rowName)
            mldb.log(colName)
            mldb.log(value)
            assert False

        # did not find row
        mldb.log(res)
        mldb.log(rowName)
        mldb.log(colName)
        mldb.log(value)
        assert False

    def do_asserts(self, row_prefix, js_res):
        assert_val = partial(self.assert_val, js_res)
        assert_val(row_prefix + "1", "colA", 1)
        assert_val(row_prefix + "1", "colB", "pwet pwet")
        assert_val(row_prefix + "2", "colB", "pwet pwet 2")

        assert_val(row_prefix + "3", "colC.a", 1)
        assert_val(row_prefix + "3", "colC.b", 2)

        assert_val(row_prefix + "4", "colD.0", "{\"a\":1}")
        assert_val(row_prefix + "4", "colD.1", "{\"b\":2}")

        assert_val(row_prefix + "5", "colD.1", 1)
        assert_val(row_prefix + "5", "colD.abc", 1)

    def test_import_json_procedure(self):
        conf = {
            "id": "json_importer",
            "type": "import.json",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
                "outputDataset": {
                    "id": "my_json_dataset",
                    "type": "sparse.mutable"
                },
                "runOnCreation": True
            }
        }
        mldb.put("/v1/procedures/json_importer", conf)

        res = mldb.get("/v1/query",
                       q="select * from my_json_dataset order by rowName()")
        self.do_asserts("", res.json())

    def test_import_invalid_json(self):
        conf = {
            "id": "json_importer",
            "type": "import.json",
            "params": {
                "dataFileUrl":
                    "file://mldb/testing/dataset/json_dataset_invalid.json",
                "outputDataset": {
                    "id": "my_json_dataset",
                    "type": "sparse.mutable"
                },
                "runOnCreation": True
            }
        }
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.put("/v1/procedures/json_importer", conf)

    def test_ignore_bad_lines(self):
        conf = {
            "id": "json_importer",
            "type": "import.json",
            "params": {
                "dataFileUrl":
                    "file://mldb/testing/dataset/json_dataset_invalid.json",
                "outputDataset": {
                    "id": "my_json_dataset2",
                    "type": "sparse.mutable"
                },
                "runOnCreation": True,
                "ignoreBadLines": True
            }
        }

        mldb.put("/v1/procedures/json_importer", conf)

        res = mldb.get("/v1/query",
                       q="select * from my_json_dataset2 order by rowName()")
        js_res = res.json()
        self.assert_val(js_res, "1", "colA", 1)
        self.assert_val(js_res, "3", "colB", "pwet pwet 2")

    def test_json_builtin_function(self):
        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'file://mldb/testing/dataset/json_dataset.json',
                "ouputDataset": {
                    "id": "imported_json",
                },
                "quotechar": "",
                "delimiter": "",
                "runOnCreation" : True,
            }
        }
        mldb.put("/v1/procedures/csv_proc", csv_conf) 

        res = mldb.get(
            "/v1/query",
            q="select parse_json(lineText, {arrays: 'encode'}) as * from imported_json")
        self.do_asserts("", res.json())

if __name__ == '__main__':
    mldb.run_tests()
