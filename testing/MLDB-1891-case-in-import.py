#
# MLDB-1891-case-in-import.py
# Mathieu Bolduc, 2016-08-12
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1891CaseInImport(MldbUnitTest):  # noqa

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

mldb.run_tests()