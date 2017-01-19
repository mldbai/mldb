#
# MLDB-1266-import_json.py
# 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

from functools import partial

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


class ImportJsonTest(MldbUnitTest):  # noqa

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
                'arrays' : 'encode'
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
                "outputDataset": {
                    "id": "imported_json",
                },
                "quoteChar": "",
                "delimiter": "",
                "runOnCreation" : True,
            }
        }
        mldb.put("/v1/procedures/csv_proc", csv_conf)

        res = mldb.get(
            "/v1/query",
            q="select parse_json(lineText, {arrays: 'encode'}) as * from imported_json")
        self.do_asserts("", res.json())

    def test_mldb_1729_output_dataset_string_def(self):
        mldb.post("/v1/procedures", {
            "type": "import.json",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
                "outputDataset": "my_json_dataset_1",
                'arrays' : 'encode'
            }
        })

        res = mldb.get("/v1/query",
                       q="SELECT * FROM my_json_dataset_1 ORDER BY rowName()")
        self.do_asserts("", res.json())

    def test_mldb_1729_output_dataset_string_def_params(self):
        """
        Make sure the defaults don't overwrite the given config.
        """
        conf = {
            "type": "import.json",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
                "outputDataset": {
                    'id' : "my_json_dataset_2",
                    'params' : {
                        'unknownColumns' : 'error'
                    }
                },
                "runOnCreation": True
            }
        }

        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.post("/v1/procedures", conf)

    def test_where_filtering(self):
        mldb.post("/v1/procedures", {
            "type": "import.json",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
                "outputDataset": {
                    'id' : "test_where_filtering",
                },
                "runOnCreation": True,
                'where' : 'colA IN (1, 2)'
            }
        })
        res = mldb.query("SELECT * FROM test_where_filtering")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB'],
            ['1', 1, 'pwet pwet'],
            ['2', 2, 'pwet pwet 2']
        ])

    def test_select(self):
        mldb.post("/v1/procedures", {
            "type": "import.json",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
                "outputDataset": {
                    'id' : "test_where_filtering",
                },
                "runOnCreation": True,
                'select' : 'colA'
            }
        })
        res = mldb.query("SELECT * FROM test_where_filtering")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA'],
            ['1', 1],
            ['2', 2],
            ['3', 3],
            ['4', None],
            ['5', None],
            ['6', None]
        ])

        mldb.post("/v1/procedures", {
            "type": "import.json",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
                "outputDataset": {
                    'id' : "test_where_filtering_2",
                },
                "runOnCreation": True,
                'select' : '* EXCLUDING (colA)'
            }
        })
        res = mldb.query("""SELECT * FROM test_where_filtering_2
                            WHERE rowName()='1'
                         """)
        self.assertTableResultEquals(res, [
            ['_rowName', 'colB'],
            ['1', 'pwet pwet']
        ])

        mldb.post("/v1/procedures", {
            "type": "import.json",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
                "outputDataset": {
                    'id' : "test_where_filtering_3",
                },
                "runOnCreation": True,
                'select' : 'colA AS wololo'
            }
        })
        res = mldb.query("""SELECT * FROM test_where_filtering_3
                            WHERE rowName()='1'
                         """)
        self.assertTableResultEquals(res, [
            ['_rowName', 'wololo'],
            ['1', 1]
        ])

    def test_named_base(self):
        mldb.post("/v1/procedures", {
            "type": "import.json",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
                "outputDataset": {
                    'id' : "test_named",
                },
                "runOnCreation": True,
                'named' : 'colB',
                'where' : 'colB IS NOT NULL'
            }
        })
        res = mldb.query("""SELECT colB FROM test_named""")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colB'],
            ['pwet pwet', 'pwet pwet'],
            ['pwet pwet 2', 'pwet pwet 2'],
            ['pwet pwet 3', 'pwet pwet 3']
        ])

    def test_named_on_object(self):
        msg = 'Cannot convert value of type'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post("/v1/procedures", {
                "type": "import.json",
                "params": {
                    "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
                    "outputDataset": {
                        'id' : "test_where_filtering_2",
                    },
                    "runOnCreation": True,
                    'named' : 'colC',
                    'where' : 'colC IS NOT NULL'
                }
            })

    def test_named_line_number_fct(self):
        mldb.post("/v1/procedures", {
            "type": "import.json",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
                "outputDataset": {
                    'id' : "test_named_line_number_fct",
                },
                "runOnCreation": True,
                'named' : 'lineNumber() - 1',
            }
        })
        res = mldb.query("SELECT colA FROM test_named_line_number_fct")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA'],
            ["0", 1],
            ["1", 2],
            ["2", 3],
            ["3", None],
            ["4", None],
            ["5", None]
        ])

    def test_no_input_file(self):
        msg = "dataFileUrl is a required property and must not be empty";
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post("/v1/procedures", {
                "type": "import.json",
                "params": {
                    "outputDataset": {
                        'id' : "test_no_input_file",
                    },
                    "runOnCreation": True,
                    'named' : 'lineNumber() - 1',
                }
            })

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post("/v1/procedures", {
                "type": "import.json",
                "params": {
                    'dataFileUrl' : '',
                    "outputDataset": {
                        'id' : "test_no_input_file",
                    },
                    "runOnCreation": True,
                    'named' : 'lineNumber() - 1',
                }
            })

        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.post("/v1/procedures", {
                "type": "import.json",
                "params": {
                    'dataFileUrl' : 'file://',
                    "outputDataset": {
                        'id' : "test_no_input_file",
                    },
                    "runOnCreation": True,
                    'named' : 'lineNumber() - 1',
                }
            })

    def test_arrays_parse(self):
        mldb.post("/v1/procedures", {
            "type": "import.json",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/json_dataset.json",
                "outputDataset": "test_arrays_parse_ds",
                'arrays' : 'parse'
            }
        })

        res = mldb.query(
            "SELECT * FROM test_arrays_parse_ds ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ["_rowName", "colA", "colB", "colC.a", "colC.b", "colD.0.a",
             "colD.1.b", "colD.0", "colD.1", "colD.2", "colD.3", "colD.4",
             "colD.5", "colC.a.b"],
            ["1", 1, "pwet pwet", None, None, None, None, None, None, None, None, None, None, None],
            ["2", 2, "pwet pwet 2", None, None, None, None, None, None, None, None, None, None, None],
            ["3", 3, "pwet pwet 3", 1, 2, None, None, None, None, None, None, None, None, None],
            ["4", None, None, None, None, 1, 2, None, None, None, None, None, None, None],
            ["5", None, None, None, None, None, None, 1, 2, 3, 0, 5.25, "abc", None],
            ["6", None, None, None, None, None, None, None, None, None, None, None, None, 2]
        ])

if __name__ == '__main__':
    mldb.run_tests()
