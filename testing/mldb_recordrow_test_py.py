# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#
# mldb_recordrow_test.py
# Francois Maillet, 2015-03-17
# Copyright (c) 2015 mldb.ai inc.  All rights reserved.
#

import unittest
import requests
import json
from mldb_py_runner.mldb_py_runner import MldbRunner

class DatasetTest(unittest.TestCase):
    def setUp(self):
        self.mldb = MldbRunner()
        self.port = self.mldb.port
        self.url = 'http://localhost:' + str(self.port) + '/v1'
        self.loadDatasetToMLDB()

    def tearDown(self):
        del self.mldb
        self.mldb = None

    def loadDatasetToMLDB(self):

        pythonScript = {
        "source":  """
datasetConfig = {
    "type": "sparse.mutable",
    "id": "testing_types"
}
dataset = mldb.create_dataset(datasetConfig)
 
import datetime
ts = datetime.datetime.now()

dataset.record_row("id_1", [['x', 1.5,   ts], ['label', '0', ts]])
dataset.record_row("id_2", [['x', 2.5,   ts], ['label', '1', ts]])
dataset.record_row("id_3", [['x', "2.5", ts], ['label', '1', ts]])
dataset.commit()

mldb.log("Commited!!")

"""
    }
        response = requests.post(self.url+"/types/plugins/python/routes/run", json.dumps(pythonScript))
        print response


    def test_dataset_biggerThan2(self):        
        response = requests.get(self.url+"/query?q=select * from testing_types where x>2")
        rez = response.json()
        print rez
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(rez), 2)
        self.assertEqual(rez[0]["rowName"], "id_3")
        self.assertEqual(rez[1]["rowName"], "id_2")
    
    def test_dataset_biggerEq2dot5(self):
        response = requests.get(self.url+"/query?q=select * from testing_types where x='2.5'")
        rez = response.json()
        print rez
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(rez), 1)
        self.assertEqual(rez[0]["rowName"], "id_3")
    
    def test_dataset_biggerSmaller2(self):
        response = requests.get(self.url+"/query?q=select * from testing_types where x<2")
        rez = response.json()
        print rez
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(rez), 1)
        self.assertEqual(rez[0]["rowName"], "id_1")

if __name__ == '__main__':
    unittest.main(verbosity=2)
