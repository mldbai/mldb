# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@datacratic.com
# @Date:               2015-03-10 10:29:49
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-01 09:19:11
# @File Name:          batframe_column_comparison_test.py


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

    def test_eq(self):

        #              This column == Male
        #                     ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  |
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------

        # Testing string equality
        bf = BatFrame(DatasetTest.dataset_url)
        df = bf[bf["Gender"] == "Male"].toPandas()
        self.assertEqual(len(df), 4)
        #rowNames = df.index
        self.assertTupleEqual(df.ix["Batman"].shape, (8,))
        self.assertTupleEqual(df.ix["Flash"].shape, (8,))
        self.assertTupleEqual(df.ix["Superman"].shape, (8,))
        self.assertTupleEqual(df.ix["Joker"].shape, (8,))

        def helper(rowName):
            df.ix[rowName]
        self.assertRaises(KeyError, helper, "Wonder Woman")
        self.assertRaises(KeyError, helper, "Catwoman")

        # Testing INTEGER equality

        #                       This column == 183
        #                             ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  |
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Height"] == 183].toPandas()
        self.assertEqual(len(df), 3)
        # Wonder Woman, Flash and Joker are all 183cm tall
        self.assertTupleEqual(df.ix["Wonder Woman"].shape, (8,))
        self.assertTupleEqual(df.ix["Flash"].shape, (8,))
        self.assertTupleEqual(df.ix["Joker"].shape, (8,))

        # Batman, Catwoman and Superman are either taller or smaller
        self.assertRaises(KeyError, helper, "Batman")
        self.assertRaises(KeyError, helper, "Catwoman")
        self.assertRaises(KeyError, helper, "Superman")

        # Testing FLOAT equality

        #                        This column == 190.5
        #                             ⇓
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
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Height"] == 190.5].toPandas()
        self.assertEqual(len(df), 1)
        # Only Superman is 190.5 cm tall
        self.assertTupleEqual(df.ix["Superman"].shape, (7,))

        # Everyone else are either taller or smaller
        self.assertRaises(KeyError, helper, "Batman")
        self.assertRaises(KeyError, helper, "Catwoman")
        self.assertRaises(KeyError, helper, "Wonder Woman")
        self.assertRaises(KeyError, helper, "Flash")
        self.assertRaises(KeyError, helper, "Joker")

        # Testing Column equality

        #                                           Those column equal
        #                                               ⇓  ==  ⇓
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
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Eyes"] == bf["Hair"]].toPandas()
        self.assertEqual(len(df), 1)
        # Only the Joker has the same color for eyes and hair
        self.assertTupleEqual(df.ix["Joker"].shape, (8,))

        # Batman, Catwoman and Superman are either taller or smaller
        self.assertRaises(KeyError, helper, "Batman")
        self.assertRaises(KeyError, helper, "Wonder Woman")
        self.assertRaises(KeyError, helper, "Flash")
        self.assertRaises(KeyError, helper, "Catwoman")
        self.assertRaises(KeyError, helper, "Superman")

    def test_ne(self):

        #              This column != Male
        #                     ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        # Testing string equality
        bf = BatFrame(DatasetTest.dataset_url)
        df = bf[bf["Gender"] != "Male"].toPandas()
        self.assertEqual(len(df), 2)

        self.assertTupleEqual(df.ix["Wonder Woman"].shape, (7,))
        self.assertTupleEqual(df.ix["Catwoman"].shape, (7,))

        def helper(rowName):
            df.ix[rowName]
        self.assertRaises(KeyError, helper, "Batman")
        self.assertRaises(KeyError, helper, "Flash")
        self.assertRaises(KeyError, helper, "Joker")
        self.assertRaises(KeyError, helper, "Superman")


        # Testing INTEGER equality

        #                       This column != 183
        #                             ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Height"] != 183].toPandas()
        self.assertEqual(len(df), 3)
        # Wonder Woman, Flash and Joker are all 183cm tall
        self.assertTupleEqual(df.ix["Batman"].shape, (7,))
        self.assertTupleEqual(df.ix["Catwoman"].shape, (7,))
        self.assertTupleEqual(df.ix["Superman"].shape, (7,))

        # Batman, Catwoman and Superman are either taller or smaller
        self.assertRaises(KeyError, helper, "Wonder Woman")
        self.assertRaises(KeyError, helper, "Flash")
        self.assertRaises(KeyError, helper, "Joker")

        # Testing FLOAT equality

        #                        This column != 190.5
        #                             ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Height"] != 190.5].toPandas()
        self.assertEqual(len(df), 5)
        # Only Superman is 190.5 cm tall
        self.assertTupleEqual(df.ix["Batman"].shape, (8,))
        self.assertTupleEqual(df.ix["Catwoman"].shape, (8,))
        self.assertTupleEqual(df.ix["Wonder Woman"].shape, (8,))
        self.assertTupleEqual(df.ix["Flash"].shape, (8,))
        self.assertTupleEqual(df.ix["Joker"].shape, (8,))

        # Everyone else are either taller or smaller
        self.assertRaises(KeyError, helper, "Superman")

        # Testing Column inequality

        #                                         Those column not equal
        #                                               ⇓  !=  ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Eyes"] != bf["Hair"]].toPandas()
        self.assertEqual(len(df), 5)
        # Everyone but the Joker don't have matching hair and eye color
        self.assertTupleEqual(df.ix["Batman"].shape, (7,))
        self.assertTupleEqual(df.ix["Wonder Woman"].shape, (7,))
        self.assertTupleEqual(df.ix["Flash"].shape, (7,))
        self.assertTupleEqual(df.ix["Catwoman"].shape, (7,))
        self.assertTupleEqual(df.ix["Superman"].shape, (7,))

        # Joker, all alone in his corner
        self.assertRaises(KeyError, helper, "Joker")

    def test_gt(self):
        bf = BatFrame(DatasetTest.dataset_url)

        # Testing INTEGER greater than

        #                       This column > 183
        #                             ⇓
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

        def helper(rowName):
            df.ix[rowName]

        df = bf[bf["Height"] > 183].toPandas()
        self.assertEqual(len(df), 2)
        # Batman and Superman are taller than 183 cm
        self.assertTupleEqual(df.ix["Batman"].shape, (7,))
        self.assertTupleEqual(df.ix["Superman"].shape, (7,))

        # All the rest are either shorter or equal to 183 cm
        self.assertRaises(KeyError, helper, "Wonder Woman")
        self.assertRaises(KeyError, helper, "Catwoman")
        self.assertRaises(KeyError, helper, "Flash")
        self.assertRaises(KeyError, helper, "Joker")


        # Testing FLOAT greater than

        #                        This column > 190.5
        #                             ⇓
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

        df = bf[bf["Height"] > 190.5].toPandas()
        self.assertEqual(len(df), 0)
        # No one is taller than 190.5 cm

        # Everyone else are either taller or smaller
        self.assertRaises(KeyError, helper, "Batman")
        self.assertRaises(KeyError, helper, "Catwoman")
        self.assertRaises(KeyError, helper, "Wonder Woman")
        self.assertRaises(KeyError, helper, "Flash")
        self.assertRaises(KeyError, helper, "Joker")
        self.assertRaises(KeyError, helper, "Superman")

        # Testing Column gt

        #                            This gt This
        #                             ⇓   >   ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Height"] > bf["Weight"]].toPandas()
        self.assertEqual(len(df), 2)

        # The tall and thin (Height > Weight)
        self.assertTupleEqual(df.ix["Wonder Woman"].shape, (7,))
        self.assertTupleEqual(df.ix["Catwoman"].shape, (7,))

        # the bulky ones
        self.assertRaises(KeyError, helper, "Batman")
        self.assertRaises(KeyError, helper, "Flash")
        self.assertRaises(KeyError, helper, "Superman")
        self.assertRaises(KeyError, helper, "Joker")

    def test_ge(self):
        bf = BatFrame(DatasetTest.dataset_url)

        # Testing INTEGER greater than or equal

        #                       This column >= 183
        #                             ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  |
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------

        def helper(rowName):
            df.ix[rowName]

        df = bf[bf["Height"] >= 183].toPandas()
        self.assertEqual(len(df), 5)
        # Batman and Superman are taller or equal to 183 cm
        self.assertTupleEqual(df.ix["Batman"].shape, (8,))
        self.assertTupleEqual(df.ix["Wonder Woman"].shape, (8,))
        self.assertTupleEqual(df.ix["Flash"].shape, (8,))
        self.assertTupleEqual(df.ix["Joker"].shape, (8,))
        self.assertTupleEqual(df.ix["Superman"].shape, (8,))

        # Only Catwoman is stricly shorter than 183 cm
        self.assertRaises(KeyError, helper, "Catwoman")

        # Testing FLOAT greater than or equal

        #                        This column >= 190.5
        #                             ⇓
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
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Height"] >= 190.5].toPandas()
        self.assertEqual(len(df), 1)
        # Only Superman is taller or equal to 190.5 cm
        self.assertTupleEqual(df.ix["Superman"].shape, (7,))

        # Everyone else are smaller
        self.assertRaises(KeyError, helper, "Batman")
        self.assertRaises(KeyError, helper, "Catwoman")
        self.assertRaises(KeyError, helper, "Wonder Woman")
        self.assertRaises(KeyError, helper, "Flash")
        self.assertRaises(KeyError, helper, "Joker")

        # Testing Column ge

        #                            This ge  This
        #                             ⇓   >=   ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Height"] >= bf["Weight"]].toPandas()
        self.assertEqual(len(df), 3)

        # The tall and thin (Height > Weight)
        self.assertTupleEqual(df.ix["Wonder Woman"].shape, (8,))
        self.assertTupleEqual(df.ix["Catwoman"].shape, (8,))
        self.assertTupleEqual(df.ix["Joker"].shape, (8,))

        # the bulky ones
        self.assertRaises(KeyError, helper, "Batman")
        self.assertRaises(KeyError, helper, "Flash")
        self.assertRaises(KeyError, helper, "Superman")

    def test_lt(self):
        bf = BatFrame(DatasetTest.dataset_url)

        # Testing INTEGER lesser than

        #                       This column < 183
        #                             ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        def helper(rowName):
            df.ix[rowName]

        df = bf[bf["Height"] < 183].toPandas()
        self.assertEqual(len(df), 1)
        # Only Catwoman is smaller than 183 cm
        self.assertTupleEqual(df.ix["Catwoman"].shape, (7,))

        # All the rest are either taller or equal to 183 cm
        self.assertRaises(KeyError, helper, "Wonder Woman")
        self.assertRaises(KeyError, helper, "Batman")
        self.assertRaises(KeyError, helper, "Flash")
        self.assertRaises(KeyError, helper, "Joker")
        self.assertRaises(KeyError, helper, "Superman")


        # Testing FLOAT lesser than

        #                        This column < 190.5
        #                             ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Height"] < 190.5].toPandas()
        self.assertEqual(len(df), 5)
        # All but Superman are smaller than 190.5 cm
        self.assertTupleEqual(df.ix["Batman"].shape, (8,))
        self.assertTupleEqual(df.ix["Catwoman"].shape, (8,))
        self.assertTupleEqual(df.ix["Wonder Woman"].shape, (8,))
        self.assertTupleEqual(df.ix["Flash"].shape, (8,))
        self.assertTupleEqual(df.ix["Joker"].shape, (8,))

        # Only Superman. He's big!
        self.assertRaises(KeyError, helper, "Superman")

        # Testing Column lt

        #                            This lt This
        #                             ⇓   <   ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  |
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Height"] < bf["Weight"]].toPandas()
        self.assertEqual(len(df), 3)

        # The bulky ones (Height > Weight)
        self.assertTupleEqual(df.ix["Batman"].shape, (7,))
        self.assertTupleEqual(df.ix["Flash"].shape, (7,))
        self.assertTupleEqual(df.ix["Superman"].shape, (7,))

        # The thin and tall
        self.assertRaises(KeyError, helper, "Wonder Woman")
        self.assertRaises(KeyError, helper, "Catwoman")
        self.assertRaises(KeyError, helper, "Joker")

    def test_le(self):
        bf = BatFrame(DatasetTest.dataset_url)

        # Testing INTEGER lesser than or equal

        #                       This column <= 183
        #                             ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------

        def helper(rowName):
            df.ix[rowName]

        df = bf[bf["Height"] <= 183].toPandas()
        self.assertEqual(len(df), 4)
        # Only Catwoman is smaller than 183 cm
        self.assertTupleEqual(df.ix["Wonder Woman"].shape, (8,))
        self.assertTupleEqual(df.ix["Flash"].shape, (8,))
        self.assertTupleEqual(df.ix["Catwoman"].shape, (8,))
        self.assertTupleEqual(df.ix["Joker"].shape, (8,))

        # Batman and Superman are the biggest
        self.assertRaises(KeyError, helper, "Batman")
        self.assertRaises(KeyError, helper, "Superman")

        # Testing FLOAT lesser than or equal

        #                        This column <= 190.5
        #                             ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Height"] <= 190.5].toPandas()
        self.assertEqual(len(df), 6)
        # All but Superman are smaller than 190.5 cm
        self.assertTupleEqual(df.ix["Batman"].shape, (8,))
        self.assertTupleEqual(df.ix["Catwoman"].shape, (8,))
        self.assertTupleEqual(df.ix["Wonder Woman"].shape, (8,))
        self.assertTupleEqual(df.ix["Flash"].shape, (8,))
        self.assertTupleEqual(df.ix["Joker"].shape, (8,))
        self.assertTupleEqual(df.ix["Superman"].shape, (8,))

        # No one is taller than 190.5

        # Testing Column le

        #                            This le This
        #                             ⇓   <=   ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  |
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------

        df = bf[bf["Height"] <= bf["Weight"]].toPandas()
        self.assertEqual(len(df), 4)

        # The bulky ones (Height > Weight)
        self.assertTupleEqual(df.ix["Batman"].shape, (8,))
        self.assertTupleEqual(df.ix["Flash"].shape, (8,))
        self.assertTupleEqual(df.ix["Superman"].shape, (8,))
        self.assertTupleEqual(df.ix["Joker"].shape, (8,))

        # The thin and tall
        self.assertRaises(KeyError, helper, "Wonder Woman")
        self.assertRaises(KeyError, helper, "Catwoman")

    def test_and(self):
        pass

if __name__ == '__main__':
    unittest.main(verbosity=2)
