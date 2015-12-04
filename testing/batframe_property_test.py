# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@datacratic.com
# @Date:               2015-03-06 11:41:32
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-14 09:44:04
# @File Name:          batframe_property_test.py


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

    def test_columns(self):
        bf = BatFrame(DatasetTest.dataset_url)
        columns = bf.columns
        self.assertEqual(len(columns), 8, columns)
        self.assertTrue("Gender" in columns)
        self.assertTrue("Height" in columns)
        self.assertTrue("Weight" in columns)
        self.assertTrue("Eyes" in columns)
        self.assertTrue("Hair" in columns)
        self.assertTrue("Skin" in columns)
        self.assertTrue("Good" in columns)
        self.assertTrue("Bad" in columns)

    def test_rows(self):
        bf = BatFrame(DatasetTest.dataset_url)
        rows = bf.rows
        self.assertEqual(len(rows), 6, rows)
        self.assertTrue("Batman" in rows)
        self.assertTrue("Wonder Woman" in rows)
        self.assertTrue("Flash" in rows)
        self.assertTrue("Catwoman" in rows)
        self.assertTrue("Superman" in rows)
        self.assertTrue("Joker" in rows)

    def test_ix(self):

        #
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  |
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------


        bf = BatFrame(DatasetTest.dataset_url)
        Wonder_Woman = bf.ix["Wonder Woman"]
        self.assertEqual(Wonder_Woman['Gender'].values[0], "Female")
        self.assertEqual(Wonder_Woman['Height'].values[0], 183)
        self.assertEqual(Wonder_Woman['Weight'].values[0], 130)
        self.assertEqual(Wonder_Woman['Eyes'].values[0], "Blue")
        self.assertEqual(Wonder_Woman['Hair'].values[0], "Black")
        self.assertTrue(Wonder_Woman['Good'].values[0])
        self.assertFalse(Wonder_Woman['Bad'].values[0])

        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  |
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        trinity = bf.ix[["Wonder Woman", "Batman", "Superman"]]
        self.assertTupleEqual(trinity.toPandas().shape, (3, 7))

        self.assertItemsEqual(trinity['Gender'].values, ["Female", "Male", "Male"])
        self.assertItemsEqual(trinity['Height'].values, [183, 188, 190.5])
        self.assertItemsEqual(trinity['Weight'].values, [130, 210, 235])
        self.assertItemsEqual(trinity['Eyes'].values, ["Blue"]*3)
        self.assertItemsEqual(trinity['Hair'].values, ["Black"]*3)
        self.assertItemsEqual(trinity['Good'].values, [True]*3)
        self.assertItemsEqual(trinity['Bad'].values, [False]*3)


        #                   These columns
        #                    ⇓        ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  |
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        trinity = bf.ix[["Batman", "Superman"], ["Gender", "Height"]].toPandas()
        self.assertTupleEqual(trinity.shape, (2, 2))

        self.assertItemsEqual(trinity['Gender'].values, ["Male", "Male"])
        self.assertItemsEqual(trinity['Height'].values, [188, 190.5])

if __name__ == '__main__':
    unittest.main(verbosity=2)
