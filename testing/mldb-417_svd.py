# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


import unittest
import requests
import json
import datetime
from mldb_py_runner.mldb_py_runner import MldbRunner


class DatasetTest(unittest.TestCase):

    @classmethod
    def setUpClass(DatasetTest):
        DatasetTest.mldb = MldbRunner()
        DatasetTest.port = DatasetTest.mldb.port
        DatasetTest.loadDatasetToMLDB()

    @classmethod
    def tearDownClass(DatasetTest):
        del DatasetTest.mldb
        DatasetTest.mldb = None

    @classmethod
    def loadDatasetToMLDB(DatasetTest):
        dataset_name = "iris_dataset"
        DatasetTest.url = "http://localhost:%d/v1" % DatasetTest.port
        DatasetTest.dataset_url = DatasetTest.url + "/datasets/" + dataset_name
        requests.delete(DatasetTest.dataset_url)

        # Register the dataset
        data = json.dumps({"type": "sparse.mutable"})
        resp = requests.put(
            DatasetTest.url+"/datasets/" + dataset_name,
            data=data)
        if resp.status_code != 201:
            raise RuntimeError("dataset was not created")

        ts = datetime.datetime.now().isoformat(' ')

        with open("./mldb/testing/dataset/iris.data") as f:
            for i, line in enumerate(f):
                cols = []
                line_split = line.split(',')
                if len(line_split) != 5:
                    continue
                cols.append(["sepal length", float(line_split[0]), ts])
                cols.append(["sepal width", float(line_split[1]), ts])
                cols.append(["petal length", float(line_split[2]), ts])
                cols.append(["petal width", float(line_split[3]), ts])
                cols.append(["class", line_split[4], ts])

                data = {"rowName": str(i+1), "columns": cols}
                response = requests.post(
                    DatasetTest.dataset_url + "/rows",
                    json.dumps(data))
                response_code = response.status_code
                if response_code != 200:
                    print response
                    raise RuntimeError("Failed to record row")

        requests.post(DatasetTest.dataset_url + "/commit")

    def test_svd(self):
        svd_procedure = "/procedures/svd_iris"
         # svd procedure configuration
        svd_config = {
            'type' : 'svd.train',
            'params' :
            {
                "trainingData": {"from" : {"id": "iris_dataset"},
                                 "select": "\"petal\", \"width\", \"sepal\", \"length\""
                                },
                "columnOutputDataset": {
                    "type": "sparse.mutable",
                    "id": "svd_iris_col"
                },
                "rowOutputDataset": {
                    "id": "svd_iris_row",
                    'type': "embedding"
                },
                "numSingularValues": 4,
                "numDenseBasisVectors": 2
            }
        }

        response = requests.put(
            DatasetTest.url + svd_procedure,
            data=json.dumps(svd_config))
        msg = "Could not create the svd procedure Got status {}\n{}"
        msg = msg.format(response.status_code, response.content)
        self.assertEqual(response.status_code, 201, msg=msg)

        print(DatasetTest.url + svd_procedure + '/runs/1')
        response = requests.put(
            DatasetTest.url + svd_procedure + '/runs/1', data='{}')
        msg = "Could not train the svd procedure Got status {}\n{}"
        msg = msg.format(response.status_code, response.content)
        self.assertTrue(300 > response.status_code >= 200, msg=msg)


if __name__ == '__main__':
    unittest.main(verbosity=2)
