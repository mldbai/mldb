#
# MLDB-1891-case-in-import.py
# Mathieu Bolduc, 2016-08-12
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1891CaseInImport(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "test", "type": "sparse.mutable" })
        ds.record_row("row", [['x', 5, 0]])
        ds.commit()


    def test_case_import(self):
        res = mldb.put('/v1/procedures/import_case', { 
            "type": "import.text",  
            "params": { 
                "dataFileUrl": "file://mldb/testing/dataset/MLDB-1638.csv",
                'delimiter':',', 
                'quoteChar':'',
                'outputDataset': 'import_case',
                'limit': 2000,
                'select': "CASE a WHEN 'patate' THEN 0 ELSE 1 END",
                'runOnCreation': True
            } 
        })  

        mldb.log(res)

    def test_case_import_column(self):

        msg = "Import select expression cannot have row-valued columns"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/procedures/import_case_column', { 
                "type": "import.text",  
                "params": { 
                    "dataFileUrl": "file://mldb/testing/dataset/MLDB-1638.csv",
                    'delimiter':',', 
                    'quoteChar':'',
                    'outputDataset': 'import_case',
                    'limit': 2000,
                    'select': "CASE a WHEN 'patate' THEN {0} ELSE 1 END",
                    'runOnCreation': True
                } 
            })

    def test_case_import_column_else(self):

        msg = "Import select expression cannot have row-valued columns"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/procedures/import_case_column', { 
                "type": "import.text",  
                "params": { 
                    "dataFileUrl": "file://mldb/testing/dataset/MLDB-1638.csv",
                    'delimiter':',', 
                    'quoteChar':'',
                    'outputDataset': 'import_case',
                    'limit': 2000,
                    'select': "CASE a WHEN 'patate' THEN 0 ELSE {0} END",
                    'runOnCreation': True
                } 
            })  

    def test_case_import_multiple(self):
        res = mldb.put('/v1/procedures/import_case', { 
            "type": "import.text",  
            "params": { 
                "dataFileUrl": "file://mldb/testing/dataset/MLDB-1638.csv",
                'delimiter':',', 
                'quoteChar':'',
                'outputDataset': 'import_case',
                'limit': 2000,
                'select': "CASE a WHEN 'patate' THEN 0 WHEN 'banane' THEN 1 ELSE 2 END",
                'runOnCreation': True
            } 
        })
    def test_case_import_column_multiple(self):
        msg = "Import select expression cannot have row-valued columns"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/procedures/import_case', { 
                "type": "import.text",  
                "params": { 
                    "dataFileUrl": "file://mldb/testing/dataset/MLDB-1638.csv",
                    'delimiter':',', 
                    'quoteChar':'',
                    'outputDataset': 'import_case',
                    'limit': 2000,
                    'select': "CASE a WHEN 'patate' THEN 0 WHEN 'banane' THEN {1} ELSE 2 END",
                    'runOnCreation': True
                } 
            })  

    def test_case_import_no_else(self):
        mldb.put('/v1/procedures/import_case', { 
            "type": "import.text",  
            "params": { 
                "dataFileUrl": "file://mldb/testing/dataset/MLDB-1638.csv",
                'delimiter':',', 
                'quoteChar':'',
                'outputDataset': 'import_case',
                'limit': 2000,
                'select': "CASE a WHEN 'patate' THEN 0 WHEN 'banane' THEN 1 END",
                'runOnCreation': True
            } 

        }) 

    def test_case_import_no_else_column(self):
        msg = "Import select expression cannot have row-valued columns"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/procedures/import_case', { 
                "type": "import.text",  
                "params": { 
                    "dataFileUrl": "file://mldb/testing/dataset/MLDB-1638.csv",
                    'delimiter':',', 
                    'quoteChar':'',
                    'outputDataset': 'import_case',
                    'limit': 2000,
                    'select': "CASE a WHEN 'patate' THEN 0 WHEN 'banane' THEN {1} END",
                    'runOnCreation': True
                } 
            })

    def test_type(self):
        res = mldb.query("select static_type(CASE x WHEN 'patate' THEN 0 WHEN 'banane' THEN {1} END) as * FROM test");
        
        expected = [
            [
                "_rowName",
                "isConstant",
                "type"
            ],
            [
                "row",
                0,
                "MLDB::VariantExpressionValueInfo"
            ]
        ]

        self.assertTableResultEquals(res, expected)

    def test_type_same(self):
        res = mldb.query("select static_type(CASE x WHEN 'patate' THEN 0 WHEN 'banane' THEN 1 END) as * FROM test");
        
        expected = [["_rowName", "isConstant", "kind", "scalar", "type"],
                    ["row", 0, "scalar", "long", "MLDB::IntegerValueInfo"]]

        self.assertTableResultEquals(res, expected)

    def test_type_column_dense(self):
        res = mldb.query("select static_known_columns(CASE x WHEN 'patate' THEN {0 as a} WHEN 'banane' THEN {1 as a} END) as * FROM test");

        expected = [
            [
                "_rowName",
                "0.columnName",
                "0.sparsity",
                "0.valueInfo.isConstant",
                "0.valueInfo.kind",
                "0.valueInfo.scalar",
                "0.valueInfo.type"
            ],
            [
                "row",
                "a",
                "dense",
                0,
                "scalar",
                "long",
                "MLDB::VariantExpressionValueInfo"
            ]
        ]

        self.assertTableResultEquals(res, expected)

    def test_type_column_sparse(self):
        res = mldb.query("select static_known_columns(CASE x WHEN 'patate' THEN 0 WHEN 'banane' THEN {1 as a} END) as * FROM test");

        expected = [
            [
                "_rowName",
                "0.columnName",
                "0.sparsity",
                "0.valueInfo.isConstant",
                "0.valueInfo.kind",
                "0.valueInfo.scalar",
                "0.valueInfo.type"
            ],
            [
                "row",
                "a",
                "sparse",
                1,
                "scalar",
                "long",
                "MLDB::IntegerValueInfo"
            ]
        ]

        self.assertTableResultEquals(res, expected)

    def test_type_column_multiple(self):
        res = mldb.query("select static_known_columns(CASE x WHEN 'patate' THEN {0 as a, 1 as b} WHEN 'banane' THEN {1 as a} END) as * FROM test");

        expected = [
            [
                "_rowName",
                "0.columnName",
                "0.sparsity",
                "0.valueInfo.isConstant",
                "0.valueInfo.kind",
                "0.valueInfo.scalar",
                "0.valueInfo.type",
                "1.columnName",
                "1.sparsity",
                "1.valueInfo.kind",
                "1.valueInfo.scalar",
                "1.valueInfo.type"
            ],
            [
                "row",
                "a",
                "dense",
                0,
                "scalar",
                "long",
                "MLDB::VariantExpressionValueInfo",
                "b",
                "sparse",
                "scalar",
                "long",
                "MLDB::IntegerValueInfo"
            ]
        ]

mldb.run_tests()
