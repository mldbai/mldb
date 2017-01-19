# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#
# mldb_merged_dataset_test.py
# Mich, 2015-03-04
# Copyright (c) 2015 mldb.ai inc.  All rights reserved.
#

import unittest
import json
import requests
from mldb_py_runner.mldb_py_runner import MldbRunner


class MldbMergedDatasetTest(unittest.TestCase):

    def setUp(self):
        self.mldb = MldbRunner()
        self.port = self.mldb.port
        self.base_url = 'http://localhost:' + str(self.port) + '/v1'

    def tearDown(self):
        del self.mldb
        self.mldb = None

    def test_merge_non_existing_dataset(self):
        """
        Create a merged dataset out of non existing datasets should not
        work. Connector sends/receives everything as text.
        """
        config = {
            'type' : "merged",
            'id' : "merged_dataset",
            'params' : {
                "datasets": [
                    {"id": "whatever_1"},
                    {"id": "whatever_2"}
                ]
            }
        }

        r = requests.post(url=self.base_url + "/datasets?sync=true",
                          data=config)
        self.assertEqual(r.status_code, 400)

    def test_merge_nothing(self):
        """
        Create a merge dataset out of nothing should not work.
        Connector sends/receives everything as text.
        """
        config = {
            'type' : "merged",
            'id' : "merged_dataset",
            'params' : {
                "datasets": [
                ]
            }
        }
        r = requests.post(url=self.base_url + "/datasets?sync=true",
                          data=config)
        self.assertEqual(r.status_code, 400)

    @unittest.expectedFailure  # FIXME
    def test_sync(self):
        """
        Sync should work even when connector sends as json.
        """
        config = {
            'type' : "merged",
            'id' : "merged_dataset",
            'params' : {
                "datasets": [
                ]
            }
        }
        r = requests.post(url=self.base_url + '/datasets?sync=true',
                          data=json.dumps(config),
                          headers={'content-type' : 'application/json'})
        self.assertEqual(r.status_code, 400)

if __name__ == '__main__':
    unittest.main(verbosity=2, buffer=True)
