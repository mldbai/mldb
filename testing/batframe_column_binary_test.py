# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@datacratic.com
# @Date:               2015-03-10 10:29:49
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-01 09:18:58
# @File Name:          batframe_column_binary_test.py


import unittest
import requests
import json
import datetime
from pymldb.data import BatFrame
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

    def test_mul(self):
        #                     This column * number
        #                             ⇓
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

        # Testing with float
        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2.2
        s = (bf["Height"] * factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88*factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83*factor)
        self.assertAlmostEqual(s["Flash"], 1.83*factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70*factor)
        self.assertAlmostEqual(s["Superman"], 1.905*factor)
        self.assertAlmostEqual(s["Joker"], 1.83*factor)

        # Testing with integer
        factor = 2
        s = (bf["Height"] * factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88*factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83*factor)
        self.assertAlmostEqual(s["Flash"], 1.83*factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70*factor)
        self.assertAlmostEqual(s["Superman"], 1.905*factor)
        self.assertAlmostEqual(s["Joker"], 1.83*factor)


        #                              This column * number
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        # Testing with float
        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2.2
        s = (bf["Weight"] * factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 210*factor)
        self.assertAlmostEqual(s["Wonder Woman"], 130*factor)
        self.assertAlmostEqual(s["Flash"], 190*factor)
        self.assertAlmostEqual(s["Catwoman"], 133*factor)
        self.assertAlmostEqual(s["Superman"], 235*factor)
        self.assertAlmostEqual(s["Joker"], 183*factor)

        # Testing with integer
        factor = 2
        s = (bf["Weight"] * factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 210*factor)
        self.assertAlmostEqual(s["Wonder Woman"], 130*factor)
        self.assertAlmostEqual(s["Flash"], 190*factor)
        self.assertAlmostEqual(s["Catwoman"], 133*factor)
        self.assertAlmostEqual(s["Superman"], 235*factor)
        self.assertAlmostEqual(s["Joker"], 183*factor)

    def test_rmul(self):
        #                     number * This column
        #                             ⇓
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

        # Testing with float
        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2.2
        s = (factor * bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor*1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor*1.83)
        self.assertAlmostEqual(s["Flash"], factor*1.83)
        self.assertAlmostEqual(s["Catwoman"], factor*1.70)
        self.assertAlmostEqual(s["Superman"], factor*1.905)
        self.assertAlmostEqual(s["Joker"], factor*1.83)

        # Testing with integer
        factor = 2
        s = (factor * bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor*1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor*1.83)
        self.assertAlmostEqual(s["Flash"], factor*1.83)
        self.assertAlmostEqual(s["Catwoman"], factor*1.70)
        self.assertAlmostEqual(s["Superman"], factor*1.905)
        self.assertAlmostEqual(s["Joker"], factor*1.83)


        #                            number * This column
        #                                      ⇓
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

        # Testing with float
        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2.2
        s = (factor * bf["Weight"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor*210)
        self.assertAlmostEqual(s["Wonder Woman"], factor*130)
        self.assertAlmostEqual(s["Flash"], factor*190)
        self.assertAlmostEqual(s["Catwoman"], factor*133)
        self.assertAlmostEqual(s["Superman"], factor*235)
        self.assertAlmostEqual(s["Joker"], factor*183)

        # Testing with integer
        factor = 2
        s = (factor * bf["Weight"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor*210)
        self.assertAlmostEqual(s["Wonder Woman"], factor*130)
        self.assertAlmostEqual(s["Flash"], factor*190)
        self.assertAlmostEqual(s["Catwoman"], factor*133)
        self.assertAlmostEqual(s["Superman"], factor*235)
        self.assertAlmostEqual(s["Joker"], factor*183)

    def test_div(self):
        #                       This column / number
        #                              ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2
        s = (bf["Height"]/factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88/factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83/factor)
        self.assertAlmostEqual(s["Flash"], 1.83/factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70/factor)
        self.assertAlmostEqual(s["Superman"], 1.905/factor)
        self.assertAlmostEqual(s["Joker"], 1.83/factor)

        factor = 2.2
        s = (bf["Height"]/factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88/factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83/factor)
        self.assertAlmostEqual(s["Flash"], 1.83/factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70/factor)
        self.assertAlmostEqual(s["Superman"], 1.905/factor)
        self.assertAlmostEqual(s["Joker"], 1.83/factor)

        #                                 This column / number
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        factor = 2
        s = (bf["Weight"]/factor).toPandas()
        # Dividing by float because mldb is converting every number
        # in columns to float
        self.assertAlmostEqual(s["Batman"], 210/float(factor))
        self.assertAlmostEqual(s["Wonder Woman"], 130/float(factor))
        self.assertAlmostEqual(s["Flash"], 190/float(factor))
        self.assertAlmostEqual(s["Catwoman"], 133/float(factor))
        self.assertAlmostEqual(s["Superman"], 235/float(factor))
        self.assertAlmostEqual(s["Joker"], 183/float(factor))

        factor = 2.2
        s = (bf["Weight"]/factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 210/factor)
        self.assertAlmostEqual(s["Wonder Woman"], 130/factor)
        self.assertAlmostEqual(s["Flash"], 190/factor)
        self.assertAlmostEqual(s["Catwoman"], 133/factor)
        self.assertAlmostEqual(s["Superman"], 235/factor)
        self.assertAlmostEqual(s["Joker"], 183/factor)

    def test_rdiv(self):
        #                    number / This column
        #                              ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2
        s = (factor/bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor/1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor/1.83)
        self.assertAlmostEqual(s["Flash"], factor/1.83)
        self.assertAlmostEqual(s["Catwoman"], factor/1.70)
        self.assertAlmostEqual(s["Superman"], factor/1.905)
        self.assertAlmostEqual(s["Joker"], factor/1.83)

        factor = 2.2
        s = (factor/bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor/1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor/1.83)
        self.assertAlmostEqual(s["Flash"], factor/1.83)
        self.assertAlmostEqual(s["Catwoman"], factor/1.70)
        self.assertAlmostEqual(s["Superman"], factor/1.905)
        self.assertAlmostEqual(s["Joker"], factor/1.83)

        #                              number / This column
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        factor = 2
        s = (factor/bf["Weight"]).toPandas()
        # Dividing by float because mldb is converting every number
        # in columns to float
        self.assertAlmostEqual(s["Batman"], float(factor)/210)
        self.assertAlmostEqual(s["Wonder Woman"], float(factor)/130)
        self.assertAlmostEqual(s["Flash"], float(factor)/190)
        self.assertAlmostEqual(s["Catwoman"], float(factor)/133)
        self.assertAlmostEqual(s["Superman"], float(factor)/235)
        self.assertAlmostEqual(s["Joker"], float(factor)/183)

        factor = 2.2
        s = (factor/bf["Weight"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor/210)
        self.assertAlmostEqual(s["Wonder Woman"], factor/130)
        self.assertAlmostEqual(s["Flash"], factor/190)
        self.assertAlmostEqual(s["Catwoman"], factor/133)
        self.assertAlmostEqual(s["Superman"], factor/235)
        self.assertAlmostEqual(s["Joker"], factor/183)

    def test_truediv(self):
        #                       This column / number
        #                              ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2
        s = (bf["Height"]/2).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88/factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83/factor)
        self.assertAlmostEqual(s["Flash"], 1.83/factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70/factor)
        self.assertAlmostEqual(s["Superman"], 1.905/factor)
        self.assertAlmostEqual(s["Joker"], 1.83/factor)

        factor = 2.2
        s = (bf["Height"]/factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88/factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83/factor)
        self.assertAlmostEqual(s["Flash"], 1.83/factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70/factor)
        self.assertAlmostEqual(s["Superman"], 1.905/factor)
        self.assertAlmostEqual(s["Joker"], 1.83/factor)

        #                                 This column / number
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        factor = 2
        s = (bf["Weight"]/factor).toPandas()
        # Dividing by float because mldb is converting every number
        # in columns to float
        self.assertAlmostEqual(s["Batman"], 210/float(factor))
        self.assertAlmostEqual(s["Wonder Woman"], 130/float(factor))
        self.assertAlmostEqual(s["Flash"], 190/float(factor))
        self.assertAlmostEqual(s["Catwoman"], 133/float(factor))
        self.assertAlmostEqual(s["Superman"], 235/float(factor))
        self.assertAlmostEqual(s["Joker"], 183/float(factor))

        factor = 2.2
        s = (bf["Weight"]/factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 210/factor)
        self.assertAlmostEqual(s["Wonder Woman"], 130/factor)
        self.assertAlmostEqual(s["Flash"], 190/factor)
        self.assertAlmostEqual(s["Catwoman"], 133/factor)
        self.assertAlmostEqual(s["Superman"], 235/factor)
        self.assertAlmostEqual(s["Joker"], 183/factor)

    def test_rtruediv(self):
        #                    number / This column
        #                              ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2
        s = (factor/bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor/1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor/1.83)
        self.assertAlmostEqual(s["Flash"], factor/1.83)
        self.assertAlmostEqual(s["Catwoman"], factor/1.70)
        self.assertAlmostEqual(s["Superman"], factor/1.905)
        self.assertAlmostEqual(s["Joker"], factor/1.83)

        factor = 2.2
        s = (factor/bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor/1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor/1.83)
        self.assertAlmostEqual(s["Flash"], factor/1.83)
        self.assertAlmostEqual(s["Catwoman"], factor/1.70)
        self.assertAlmostEqual(s["Superman"], factor/1.905)
        self.assertAlmostEqual(s["Joker"], factor/1.83)

        #                              number / This column
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        factor = 2
        s = (factor/bf["Weight"]).toPandas()
        # Dividing by float because mldb is converting every number
        # in columns to float
        self.assertAlmostEqual(s["Batman"], float(factor)/210)
        self.assertAlmostEqual(s["Wonder Woman"], float(factor)/130)
        self.assertAlmostEqual(s["Flash"], float(factor)/190)
        self.assertAlmostEqual(s["Catwoman"], float(factor)/133)
        self.assertAlmostEqual(s["Superman"], float(factor)/235)
        self.assertAlmostEqual(s["Joker"], float(factor)/183)

        factor = 2.2
        s = (factor/bf["Weight"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor/210)
        self.assertAlmostEqual(s["Wonder Woman"], factor/130)
        self.assertAlmostEqual(s["Flash"], factor/190)
        self.assertAlmostEqual(s["Catwoman"], factor/133)
        self.assertAlmostEqual(s["Superman"], factor/235)
        self.assertAlmostEqual(s["Joker"], factor/183)

    def test_sub(self):
        #                       This column - number
        #                              ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        factor = 1
        s = (bf["Height"]-factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88-factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83-factor)
        self.assertAlmostEqual(s["Flash"], 1.83-factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70-factor)
        self.assertAlmostEqual(s["Superman"], 1.905-factor)
        self.assertAlmostEqual(s["Joker"], 1.83-factor)

        factor = 1.1
        s = (bf["Height"]-factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88-factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83-factor)
        self.assertAlmostEqual(s["Flash"], 1.83-factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70-factor)
        self.assertAlmostEqual(s["Superman"], 1.905-factor)
        self.assertAlmostEqual(s["Joker"], 1.83-factor)

        #                                 This column - number
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        factor = 1
        s = (bf["Weight"]-factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 210-factor)
        self.assertAlmostEqual(s["Wonder Woman"], 130-factor)
        self.assertAlmostEqual(s["Flash"], 190-factor)
        self.assertAlmostEqual(s["Catwoman"], 133-factor)
        self.assertAlmostEqual(s["Superman"], 235-factor)
        self.assertAlmostEqual(s["Joker"], 183-factor)

        factor = 1.1
        s = (bf["Weight"]-factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 210-factor)
        self.assertAlmostEqual(s["Wonder Woman"], 130-factor)
        self.assertAlmostEqual(s["Flash"], 190-factor)
        self.assertAlmostEqual(s["Catwoman"], 133-factor)
        self.assertAlmostEqual(s["Superman"], 235-factor)
        self.assertAlmostEqual(s["Joker"], 183-factor)

    def test_rsub(self):
        #                    number - This column
        #                              ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2
        s = (factor-bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor-1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor-1.83)
        self.assertAlmostEqual(s["Flash"], factor-1.83)
        self.assertAlmostEqual(s["Catwoman"], factor-1.70)
        self.assertAlmostEqual(s["Superman"], factor-1.905)
        self.assertAlmostEqual(s["Joker"], factor-1.83)

        factor = 2.2
        s = (factor-bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor-1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor-1.83)
        self.assertAlmostEqual(s["Flash"], factor-1.83)
        self.assertAlmostEqual(s["Catwoman"], factor-1.70)
        self.assertAlmostEqual(s["Superman"], factor-1.905)
        self.assertAlmostEqual(s["Joker"], factor-1.83)

        #                              number - This column
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        factor = 2
        s = (factor-bf["Weight"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor-210)
        self.assertAlmostEqual(s["Wonder Woman"], factor-130)
        self.assertAlmostEqual(s["Flash"], factor-190)
        self.assertAlmostEqual(s["Catwoman"], factor-133)
        self.assertAlmostEqual(s["Superman"], factor-235)
        self.assertAlmostEqual(s["Joker"], factor-183)

        factor = 2.2
        s = (factor-bf["Weight"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor-210)
        self.assertAlmostEqual(s["Wonder Woman"], factor-130)
        self.assertAlmostEqual(s["Flash"], factor-190)
        self.assertAlmostEqual(s["Catwoman"], factor-133)
        self.assertAlmostEqual(s["Superman"], factor-235)
        self.assertAlmostEqual(s["Joker"], factor-183)

    def test_add(self):
        #                       This column + number
        #                              ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2
        s = (bf["Height"]+2).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88+factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83+factor)
        self.assertAlmostEqual(s["Flash"], 1.83+factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70+factor)
        self.assertAlmostEqual(s["Superman"], 1.905+factor)
        self.assertAlmostEqual(s["Joker"], 1.83+factor)

        factor = 2.2
        s = (bf["Height"]+factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88+factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83+factor)
        self.assertAlmostEqual(s["Flash"], 1.83+factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70+factor)
        self.assertAlmostEqual(s["Superman"], 1.905+factor)
        self.assertAlmostEqual(s["Joker"], 1.83+factor)

        #                                 This column + number
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        factor = 2
        s = (bf["Weight"]+factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 210+factor)
        self.assertAlmostEqual(s["Wonder Woman"], 130+factor)
        self.assertAlmostEqual(s["Flash"], 190+factor)
        self.assertAlmostEqual(s["Catwoman"], 133+factor)
        self.assertAlmostEqual(s["Superman"], 235+factor)
        self.assertAlmostEqual(s["Joker"], 183+factor)

        factor = 2.2
        s = (bf["Weight"]+factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 210+factor)
        self.assertAlmostEqual(s["Wonder Woman"], 130+factor)
        self.assertAlmostEqual(s["Flash"], 190+factor)
        self.assertAlmostEqual(s["Catwoman"], 133+factor)
        self.assertAlmostEqual(s["Superman"], 235+factor)
        self.assertAlmostEqual(s["Joker"], 183+factor)

    def test_radd(self):
        #                    number + This column
        #                              ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2
        s = (factor+bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor+1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor+1.83)
        self.assertAlmostEqual(s["Flash"], factor+1.83)
        self.assertAlmostEqual(s["Catwoman"], factor+1.70)
        self.assertAlmostEqual(s["Superman"], factor+1.905)
        self.assertAlmostEqual(s["Joker"], factor+1.83)

        factor = 2.2
        s = (factor+bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor+1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor+1.83)
        self.assertAlmostEqual(s["Flash"], factor+1.83)
        self.assertAlmostEqual(s["Catwoman"], factor+1.70)
        self.assertAlmostEqual(s["Superman"], factor+1.905)
        self.assertAlmostEqual(s["Joker"], factor+1.83)

        #                              number + This column
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        factor = 2
        s = (factor+bf["Weight"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor+210)
        self.assertAlmostEqual(s["Wonder Woman"], factor+130)
        self.assertAlmostEqual(s["Flash"], factor+190)
        self.assertAlmostEqual(s["Catwoman"], factor+133)
        self.assertAlmostEqual(s["Superman"], factor+235)
        self.assertAlmostEqual(s["Joker"], factor+183)

        factor = 2.2
        s = (factor+bf["Weight"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor+210)
        self.assertAlmostEqual(s["Wonder Woman"], factor+130)
        self.assertAlmostEqual(s["Flash"], factor+190)
        self.assertAlmostEqual(s["Catwoman"], factor+133)
        self.assertAlmostEqual(s["Superman"], factor+235)
        self.assertAlmostEqual(s["Joker"], factor+183)

    def test_pow(self):
        #                       This column ** number
        #                              ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2
        s = (bf["Height"]**2).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88**factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83**factor)
        self.assertAlmostEqual(s["Flash"], 1.83**factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70**factor)
        self.assertAlmostEqual(s["Superman"], 1.905**factor)
        self.assertAlmostEqual(s["Joker"], 1.83**factor)

        factor = 2.2
        s = (bf["Height"]**factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88**factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83**factor)
        self.assertAlmostEqual(s["Flash"], 1.83**factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70**factor)
        self.assertAlmostEqual(s["Superman"], 1.905**factor)
        self.assertAlmostEqual(s["Joker"], 1.83**factor)

        #                                 This column ** number
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        factor = 2
        s = (bf["Weight"]**factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 210**factor)
        self.assertAlmostEqual(s["Wonder Woman"], 130**factor)
        self.assertAlmostEqual(s["Flash"], 190**factor)
        self.assertAlmostEqual(s["Catwoman"], 133**factor)
        self.assertAlmostEqual(s["Superman"], 235**factor)
        self.assertAlmostEqual(s["Joker"], 183**factor)

        factor = 2.2
        s = (bf["Weight"]**factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 210**factor)
        self.assertAlmostEqual(s["Wonder Woman"], 130**factor)
        self.assertAlmostEqual(s["Flash"], 190**factor)
        self.assertAlmostEqual(s["Catwoman"], 133**factor)
        self.assertAlmostEqual(s["Superman"], 235**factor)
        self.assertAlmostEqual(s["Joker"], 183**factor)

    def test_rpow(self):
        #                    number ** This column
        #                              ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2
        s = (factor**bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor**1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor**1.83)
        self.assertAlmostEqual(s["Flash"], factor**1.83)
        self.assertAlmostEqual(s["Catwoman"], factor**1.70)
        self.assertAlmostEqual(s["Superman"], factor**1.905)
        self.assertAlmostEqual(s["Joker"], factor**1.83)

        factor = 2.2
        s = (factor**bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor**1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor**1.83)
        self.assertAlmostEqual(s["Flash"], factor**1.83)
        self.assertAlmostEqual(s["Catwoman"], factor**1.70)
        self.assertAlmostEqual(s["Superman"], factor**1.905)
        self.assertAlmostEqual(s["Joker"], factor**1.83)

        #                              number ** This column
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        factor = 1
        s = (factor**bf["Weight"]).toPandas()
        # Dividing by float because mldb is converting every number
        # in columns to float
        self.assertAlmostEqual(s["Batman"], factor**210)
        self.assertAlmostEqual(s["Wonder Woman"], factor**130)
        self.assertAlmostEqual(s["Flash"], factor**190)
        self.assertAlmostEqual(s["Catwoman"], factor**133)
        self.assertAlmostEqual(s["Superman"], factor**235)
        self.assertAlmostEqual(s["Joker"], factor**183)

        factor = 1.1
        s = (factor**bf["Weight"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor**210)
        self.assertAlmostEqual(s["Wonder Woman"], factor**130)
        self.assertAlmostEqual(s["Flash"], factor**190)
        self.assertAlmostEqual(s["Catwoman"], factor**133)
        self.assertAlmostEqual(s["Superman"], factor**235)
        self.assertAlmostEqual(s["Joker"], factor**183)

    def test_mod(self):
        #                       This column % number
        #                              ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2
        s = (bf["Height"] % factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88 % factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83 % factor)
        self.assertAlmostEqual(s["Flash"], 1.83 % factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70 % factor)
        self.assertAlmostEqual(s["Superman"], 1.905 % factor)
        self.assertAlmostEqual(s["Joker"], 1.83 % factor)

        factor = 2.2
        s = (bf["Height"] % factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 1.88 % factor)
        self.assertAlmostEqual(s["Wonder Woman"], 1.83 % factor)
        self.assertAlmostEqual(s["Flash"], 1.83 % factor)
        self.assertAlmostEqual(s["Catwoman"], 1.70 % factor)
        self.assertAlmostEqual(s["Superman"], 1.905 % factor)
        self.assertAlmostEqual(s["Joker"], 1.83 % factor)

        #                                 This column % number
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        factor = 2
        s = (bf["Weight"] % factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 210 % factor)
        self.assertAlmostEqual(s["Wonder Woman"], 130 % factor)
        self.assertAlmostEqual(s["Flash"], 190 % factor)
        self.assertAlmostEqual(s["Catwoman"], 133 % factor)
        self.assertAlmostEqual(s["Superman"], 235 % factor)
        self.assertAlmostEqual(s["Joker"], 183 % factor)

        factor = 2.2
        s = (bf["Weight"] % factor).toPandas()
        self.assertAlmostEqual(s["Batman"], 210 % factor)
        self.assertAlmostEqual(s["Wonder Woman"], 130 % factor)
        self.assertAlmostEqual(s["Flash"], 190 % factor)
        self.assertAlmostEqual(s["Catwoman"], 133 % factor)
        self.assertAlmostEqual(s["Superman"], 235 % factor)
        self.assertAlmostEqual(s["Joker"], 183 % factor)

    def test_rmod(self):
        #                    number % This column
        #                              ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        factor = 2
        s = (factor % bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor % 1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor % 1.83)
        self.assertAlmostEqual(s["Flash"], factor % 1.83)
        self.assertAlmostEqual(s["Catwoman"], factor % 1.70)
        self.assertAlmostEqual(s["Superman"], factor % 1.905)
        self.assertAlmostEqual(s["Joker"], factor % 1.83)

        factor = 2.2
        s = (factor % bf["Height"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor % 1.88)
        self.assertAlmostEqual(s["Wonder Woman"], factor % 1.83)
        self.assertAlmostEqual(s["Flash"], factor % 1.83)
        self.assertAlmostEqual(s["Catwoman"], factor % 1.70)
        self.assertAlmostEqual(s["Superman"], factor % 1.905)
        self.assertAlmostEqual(s["Joker"], factor % 1.83)

        #                              number % This column
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
        # | Superman     | Male   |  1.905  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  1.83   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        factor = 2
        s = (factor % bf["Weight"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor % 210)
        self.assertAlmostEqual(s["Wonder Woman"], factor % 130)
        self.assertAlmostEqual(s["Flash"], factor % 190)
        self.assertAlmostEqual(s["Catwoman"], factor % 133)
        self.assertAlmostEqual(s["Superman"], factor % 235)
        self.assertAlmostEqual(s["Joker"], factor % 183)

        factor = 2.2
        s = (factor % bf["Weight"]).toPandas()
        self.assertAlmostEqual(s["Batman"], factor % 210)
        self.assertAlmostEqual(s["Wonder Woman"], factor % 130)
        self.assertAlmostEqual(s["Flash"], factor % 190)
        self.assertAlmostEqual(s["Catwoman"], factor % 133)
        self.assertAlmostEqual(s["Superman"], factor % 235)
        self.assertAlmostEqual(s["Joker"], factor % 183)

    def test_rand(self):
        pass
        # self.fail("Test not implemented yet")

    def test_ror(self):
        pass
        # self.fail("Test not implemented yet")


if __name__ == '__main__':
    unittest.main(verbosity=2)
