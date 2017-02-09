#
# MLDB-2138-google-cloud-storage.py
# Mathieu Marquis Bolduc Feb 08 2017
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb2138(MldbUnitTest):
    @classmethod
    def setUpClass(self):
        pass

    def test_import_gcs(self):

        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'gcs://mldb-test-data/sample.csv',
                "outputDataset": {
                    "id": "x",
                },
                "runOnCreation": True,
                "ignoreBadLines": False
            }
        }
        res = mldb.put("/v1/procedures/csv_proc", csv_conf)

        mldb.log(res)

        mldb.log(mldb.query("SELECT * FROM x"))

    def test_import_private_no_access(self):

        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'gcs://mldb-test-data/sample_private.csv',
                "outputDataset": {
                    "id": "x2",
                },
                "runOnCreation": True,
                "ignoreBadLines": False
            }
        }
        res = mldb.put("/v1/procedures/csv_proc_2", csv_conf)

        mldb.log(res)

        mldb.log(mldb.query("SELECT * FROM x2"))

mldb.run_tests()