# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@datacratic.com
# @Date:               2015-03-10 12:51:56
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-01 09:19:05
# @File Name:          batframe_column_casting_test.py


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
        dataset_name = "justice_league"
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

        # Fill the data
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  |
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        ts = datetime.datetime.now().isoformat(' ')

        def record_row(name, **kwargs):
            cols = []
            for key, val in kwargs.items():
                cols.append([key, val, ts])

            data = {"rowName": name, "columns": cols}
            response = requests.post(
                    DatasetTest.dataset_url + "/rows",
                    json.dumps(data))
            response_code = response.status_code
            if response_code != 200:
                print response
                raise RuntimeError("Failed to record row")

        record_row(
            "Batman",
            Gender="Male",
            Height=188,
            Weight=210,
            Eyes="Blue",
            Hair="Black",
            Good=True,
            Bad=False)

        record_row(
            "Wonder Woman",
            Gender="Female",
            Height=183,
            Weight=130,
            Eyes="Blue",
            Hair="Black",
            Good=True,
            Bad=False)

        record_row(
            "Flash",
            Gender="Male",
            Height=183,
            Weight=190,
            Eyes="Green",
            Hair="Red",
            Good=True,
            Bad=False)

        record_row(
            "Catwoman",
            Gender="Female",
            Height=170,
            Weight=133,
            Eyes="Green",
            Hair="Black",
            Good=True,
            Bad=True)

        record_row(
            "Superman",
            Gender="Male",
            Height=190.5,
            Weight=235,
            Eyes="Blue",
            Hair="Black",
            Good=True,
            Bad=False)

        record_row(
            "Joker",
            Gender="Male",
            Height=183,
            Weight=183,
            Eyes="Green",
            Hair="Green",
            Skin="White",
            Good=False,
            Bad=True)

        # Commit the dataset
        requests.post(DatasetTest.dataset_url + "/commit")

    def test_float(self):
        pass
        # self.fail("Test not implemented yet")

    def test_int(self):
        pass
        # self.fail("Test not implemented yet")

    def test_long(self):
        pass
        # self.fail("Test not implemented yet")

if __name__ == '__main__':
    unittest.main(verbosity=2)
