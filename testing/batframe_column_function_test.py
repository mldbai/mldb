# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@datacratic.com
# @Date:               2015-03-10 14:37:27
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-01 09:19:17
# @File Name:          batframe_column_function_test.py


import unittest
import requests
import json
import datetime
from mldb_py_runner.mldb_py_runner import MldbRunner
from pymldb.data import BatFrame


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
        # | Batman       | Male   |  1.88   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  1.83   |  130   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  1.83   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  1.70   |  133   | Green | Black |       | True  | True  |
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        ts = datetime.datetime.now().isoformat(' ')

        def record_row(name, **kwargs):
            cols = []
            for key, val in kwargs.items():
                cols.append([key, val, ts])

            data = {"rowName": name, "columns": cols}
            requests.post(
                DatasetTest.dataset_url + "/rows",
                json.dumps(data))

        record_row(
            "Batman",
            Gender="Male",
            Height=1.88,
            Weight=210,
            Eyes="Blue",
            Hair="Black",
            Good=True,
            Bad=False)

        record_row(
            "Wonder Woman",
            Gender="Female",
            Height=1.83,
            Weight=130,
            Eyes="Blue",
            Hair="Black",
            Good=True,
            Bad=False)

        record_row(
            "Flash",
            Gender="Male",
            Height=1.83,
            Weight=190,
            Eyes="Green",
            Hair="Red",
            Good=True,
            Bad=False)

        record_row(
            "Catwoman",
            Gender="Female",
            Height=1.70,
            Weight=133,
            Eyes="Green",
            Hair="Black",
            Good=True,
            Bad=True)

        record_row(
            "Superman",
            Gender="Male",
            Height=1.905,
            Weight=235,
            Eyes="Blue",
            Hair="Black",
            Good=True,
            Bad=False)

        record_row(
            "Joker",
            Gender="Male",
            Height=1.83,
            Weight=183,
            Eyes="Green",
            Hair="Green",
            Skin="White",
            Good=False,
            Bad=True)

        # Commit the dataset
        requests.post(DatasetTest.dataset_url + "/commit")

    def test_max(self):
        #                               max(This column)
        #                                       ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  1.88   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  1.83   |  130   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  1.83   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  1.70   |  133   | Green | Black |       | True  | True  |
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 1.905   |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        bf = BatFrame(DatasetTest.dataset_url)
        self.assertEqual(bf["Weight"].max(), 235)
        self.assertEqual(max(bf["Weight"]), 235)

    def test_min(self):
        #                                min(This column)
        #                                       ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  1.88   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  1.83   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  1.83   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  1.70   |  133   | Green | Black |       | True  | True  |
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        bf = BatFrame(DatasetTest.dataset_url)
        self.assertEqual(bf["Weight"].min(), 130)
        self.assertEqual(min(bf["Weight"]), 130)

    def test_unique(self):
        bf = BatFrame(DatasetTest.dataset_url)

        height = bf['Height'].unique()
        e_height = [1.88, 1.83, 1.70, 1.905]
        msg = "Got {} different values ({}) but expected 4".format(
            len(height), height)
        self.assertEqual(len(height), 4, msg=msg)
        for h in height:
            msg = "{} not in {}".format(h, e_height)
            self.assertTrue(h in e_height, msg=msg)

        weight = bf['Weight'].unique()
        e_weight = [210, 130, 190, 133, 235, 183]
        msg = "Got {} different values ({}) but expected 6".format(
            len(weight), weight)
        self.assertEqual(len(weight), 6, msg=msg)
        for w in weight:
            msg = "{} not in {}".format(w, e_weight)
            self.assertTrue(w in e_weight, msg=msg)

    def test_head(self):
        bf = BatFrame(DatasetTest.dataset_url)
        head = bf['Gender'].head()
        self.assertEqual(len(head), 5)

if __name__ == '__main__':
    unittest.main(verbosity=2)
