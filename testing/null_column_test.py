# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Copyright (c) 2015 mldb.ai inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Date:               2015-03-03 10:14:36
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-01 09:25:50
# @File Name:          null_column_test.py


import unittest
import requests
import os
import json
import datetime

from mldb_py_runner.mldb_py_runner import MldbRunner

class DatasetTest(unittest.TestCase):


    @classmethod
    def setUpClass(cls):
        cls.mldb = MldbRunner()
        cls.port = cls.mldb.port
        cls.url = "http://localhost:%d/v1" % cls.port
        cls.loadDatasetToMLDB()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists('null_column_test.beh.gz'):
            os.remove('null_column_test.beh.gz')

    @classmethod
    def loadDatasetToMLDB(cls):
        cls.dataset_name = "null_column_test"
        cls.url = "http://localhost:%d/v1" % cls.port
        cls.dataset_url = cls.url + "/datasets/" + cls.dataset_name
        requests.delete(cls.dataset_url)

        # Register the dataset
        data = json.dumps({
            "type": "sparse.mutable",
            "id": cls.dataset_name,
        })
        requests.post(cls.url + "/datasets", data=data)

        # Fill the data
        #  ___________________
        # |     | col1 | col2 |
        #  -------------------
        # | r1  |  1   |      |
        #  -------------------
        # | r2  |  1   |  2   |
        #  -------------------

        ts = datetime.datetime.now().isoformat(' ')

        cols = [['col1', 1, ts]]
        data = {"rowName": "r1", "columns": cols}
        requests.post(cls.dataset_url + "/rows", json.dumps(data))

        cols = [['col1', 1, ts], ['col2', 2, ts]]
        data = {"rowName": "r2", "columns": cols}
        requests.post(cls.dataset_url + "/rows", json.dumps(data))

        # Commit the dataset
        requests.post(cls.dataset_url + "/commit")

    def test_not_null_column(self):
        #             This column
        #                 ⇓
        #  ___________________
        # |     | col1 | col2 |
        #  -------------------
        # | r1  |  1   |      |
        #  -------------------
        # | r2  |  1   |  2   | ⇐ This row
        #  -------------------
        response = requests.get(self.dataset_url)
        datasets = json.loads(response.content)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(datasets['status']['rowCount'], 2)

        params = {
            "q": "SELECT col2 FROM " + self.dataset_name + " WHERE col2 IS NOT NULL"
        }

        response = requests.get(self.url + '/query', params=params)
        content = json.loads(response.content)

        self.assertEqual(
            len(content), 1,
            msg="Received answer {}".format(content))

        self.assertTrue('rowName' in content[0])
        rowName = content[0]['rowName']
        self.assertEqual(rowName, 'r2')

        self.assertTrue('columns' in content[0])
        columns = content[0]['columns']
        self.assertEqual(len(columns), 1)
        column = columns[0]
        self.assertEqual(column[0], 'col2')
        self.assertEqual(column[1], 2)

    def test_null_column(self):
        #      This column
        #          ⇓
        #  ___________________
        # |     | col1 | col2 |
        #  -------------------
        # | r1  |  1   |      | ⇐ This row
        #  -------------------
        # | r2  |  1   |  2   |
        #  -------------------

        response = requests.get(self.dataset_url)
        datasets = json.loads(response.content)
        self.assertEqual(
            response.status_code, 200,
            msg="Couldn't get the list of datasets")
        self.assertEqual(
            datasets['status']['rowCount'], 2,
            msg="2 rows should be in the dataset. r1 and r2")

        params = {
            "q": "SELECT col1 FROM " + self.dataset_name + " WHERE col2 IS NULL"
        }

        response = requests.get(self.url + '/query', params=params)
        content = json.loads(response.content)

        self.assertEqual(len(content), 1)

        self.assertTrue('rowName' in content[0])
        rowName = content[0]['rowName']
        self.assertEqual(rowName, 'r1')

        self.assertTrue('columns' in content[0])
        columns = content[0]['columns']
        self.assertEqual(len(columns), 1)
        column = columns[0]
        self.assertEqual(column[0], 'col1')
        self.assertEqual(column[1], 1)

    def test_where_3_way_logic(self):
        #             This column
        #                 ⇓
        #  ___________________
        # |     | col1 | col2 |
        #  -------------------
        # | r1  |  1   |      | ⇐ This row
        #  -------------------
        # | r2  |  1   |  2   |
        #  -------------------

        response = requests.get(self.dataset_url)
        datasets = json.loads(response.content)
        self.assertEqual(response.status_code, 200,
            msg="Couldn't get the list of datasets")
        self.assertEqual(datasets['status']['rowCount'], 2,
            msg="2 rows should be in the dataset. r1 and r2")

        params = {
            "q": "SELECT col1 FROM " + self.dataset_name + " WHERE col2 < 2" 
        }

        response = requests.get(self.url + '/query', params=params)
        content = json.loads(response.content)
        self.assertEqual(len(content), 0,
            msg="The query should have returned no results")


    def test_no_null_value_returned(self):
        #            This column
        #                 ⇓
        #  ___________________
        # |     | col1 | col2 |
        #  -------------------
        # | r1  |  1   |      |
        #  -------------------
        # | r2  |  1   |  2   | ⇐ This row
        #  -------------------

        response = requests.get(self.dataset_url)
        datasets = json.loads(response.content)

        self.assertEqual(
            response.status_code, 200,
            msg="Couldn't get the list of datasets")
        self.assertEqual(
            datasets['status']['rowCount'], 2,
            msg="2 rows should be in the dataset. r1 and r2")

        params = {
            "q": "SELECT col2 FROM " + self.dataset_name + " WHERE col2 <= 2" 
        }

        response = requests.get(self.url + '/query', params=params)
        content = json.loads(response.content)

        self.assertEqual(
            len(content), 1,
            msg="There should be only one row. "
            "Received answer {}".format(content))

        self.assertTrue('rowName' in content[0])
        rowName = content[0]['rowName']
        self.assertEqual(rowName, 'r2')

        self.assertTrue('columns' in content[0])
        columns = content[0]['columns']
        self.assertEqual(
            len(columns), 1,
            msg="There should be only one column. "
            "Received answer {}".format(columns))
        column = columns[0]
        self.assertEqual(column[0], 'col2')
        self.assertEqual(column[1], 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
