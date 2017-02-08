#
# MLDB-1703 null char import
# 2 juin 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1703(MldbUnitTest):  
    @classmethod
    def setUpClass(self):
        # this file includes a problematic character
        mldb.log(mldb.put("/v1/procedures/importer", {
            "type": "import.text",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/MDLB-1703_data.csv",
                "outputDataset": "test_case",
                "named": "rowName",
                "select": "* EXCLUDING(rowName)",
                "runOnCreation": True,
                "structuredColumnNames": True,
                "allowMultiLines": False,
                "replaceInvalidCharactersWith": " "
            }
        }))

    def test_select(self):
        mldb.query("select name from test_case")

    def test_tokenize(self):
        mldb.query("select tokenize(name) from test_case")

mldb.run_tests()
