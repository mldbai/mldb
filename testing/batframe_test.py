# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@datacratic.com
# @Date:               2015-03-06 11:41:32
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-14 08:15:06
# @File Name:          batframe_test.py


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

    def test_slicing(self):
        # VERY IMPORTANT
        # Slicing calls the *limit* REST parameter which will limit the number
        # of rows returned based on the **hash** of the row name. The insertion
        # order **does not** matter

        bf = BatFrame(DatasetTest.dataset_url)

        # Testing slicing [:3]

        #                                ALL THE COLUMNS
        #                    ⇓        ⇓       ⇓       ⇓       ⇓      ⇓
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
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False | ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        df = bf[:3].toPandas()
        # Right now the column Skin does not come out because no result have
        # that column
        self.assertTupleEqual(df.shape, (3, 7))
        rowNames = df.index.values.tolist()
        expected = expected = ["Superman", "Wonder Woman", "Catwoman"]
        for hero in expected:
            msg = "{} expected to be in the results".format(hero)
            self.assertTrue(hero in rowNames, msg=msg)


        # for i, hero in enumerate(expected):
        #     msg = "\n-------------Expecting---------------\n"
        #     msg += " ".join(expected)
        #     msg += "\n\n--------------Instead got--------------\n"
        #     msg += str(" ".join(rowNames))
        #     self.assertEqual(rowNames[i], hero, msg=msg)

        # Testing slicing [2:5]

        #                                ALL THE COLUMNS
        #                    ⇓        ⇓       ⇓       ⇓       ⇓      ⇓
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

        df = bf[2:5].toPandas()
        self.assertTupleEqual(df.shape, (4, 8))
        rowNames = df.index.values.tolist()
        expected = ["Flash", "Batman", "Catwoman", "Joker"]
        for hero in expected:
            msg = "{} expected to be in the results".format(hero)
            self.assertTrue(hero in rowNames, msg=msg)


        # Testing slicing [2:5]

        #                                ALL THE COLUMNS
        #                    ⇓        ⇓       ⇓       ⇓       ⇓      ⇓
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

        df = bf[4:].toPandas()
        self.assertTupleEqual(df.shape, (2, 8))
        rowNames = df.index.values.tolist()
        expected = ["Flash", "Joker"]
        for hero in expected:
            msg = "{} expected to be in the results".format(hero)
            self.assertTrue(hero in rowNames, msg=msg)

    def test_alignment(self):
        bf = BatFrame(DatasetTest.dataset_url)
        height = bf['Height'].values
        weight = bf['Weight'].values
        msg = "Got heights: {} \nand weights: {}\n Length of {} and {}"
        msg = msg.format(height, weight, len(height), len(weight))
        self.assertTrue(len(height) == len(weight), msg=msg)

        expected = [
            ("Batman", 188, 210),
            ("Wonder Woman", 183, 130),
            ("Flash", 183, 190),
            ("Catwoman", 170, 133),
            ("Superman", 190.5, 235),
            ("Joker", 183, 183)]

        for t in zip(bf.rows, height, weight):
            msg = "{} is not in the expected values {}".format(t, expected)
            self.assertTrue(t in expected, msg=msg)

    def test_head(self):
        bf = BatFrame(DatasetTest.dataset_url)
        head = bf.head().toPandas()
        self.assertTupleEqual(head.shape, (5, 8))

    def test_sort(self):

        #                             Sort on this column
        #                                      ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        sort = bf.sort("Weight")  # ascending=True is default
        rows = sort.rows
        expected_rows =[
            "Wonder Woman",
            "Catwoman",
            "Joker",
            "Flash",
            "Batman",
            "Superman"]
        for row, e_row in zip(rows, expected_rows):
            self.assertEqual(row, e_row)

        values = sort["Weight"].values
        expected_values = [130, 133, 183, 190, 210, 235]
        for value, e_value in zip(values, expected_values):
            self.assertEqual(value, e_value)

        # Reverse sort aka descending
        sort = bf.sort("Weight", ascending=False)
        rows = sort.rows
        for row, e_row in zip(rows, reversed(expected_rows)):
            self.assertEqual(row, e_row)

        values = sort["Weight"].values
        for value, e_value in zip(values, reversed(expected_values)):
            self.assertEqual(value, e_value)


        #                         Sort on those column
        #                              ⇓       ⇓
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

        bf = BatFrame(DatasetTest.dataset_url)
        sort = bf.sort(["Height", "Weight"])  # ascending=True is default
        rows = sort.rows
        expected_rows = [
            "Catwoman",
            "Wonder Woman",
            "Joker",
            "Flash",
            "Batman",
            "Superman"]
        for row, e_row in zip(rows, expected_rows):
            self.assertEqual(row, e_row)

        heights = sort["Height"].values
        expected_height = [170, 183, 183, 183, 188, 190.5]
        for height, e_height in zip(heights, expected_height):
            self.assertEqual(height, e_height)

        weights = sort["Weight"].values
        expected_weight = [133, 130, 183, 190, 210, 235]
        for weight, e_weight in zip(weights, expected_weight):
            self.assertEqual(weight, e_weight)

        # Reverse sort aka descending. Both are descending
        sort = bf.sort(["Height", "Weight"], ascending=False)
        rows = sort.rows
        for row, e_row in zip(rows, reversed(expected_rows)):
            self.assertEqual(row, e_row)

        heights = sort["Height"].values
        for height, e_height in zip(heights, reversed(expected_height)):
            self.assertEqual(height, e_height)

        weights = sort["Weight"].values
        for weight, e_weight in zip(weights, reversed(expected_weight)):
            self.assertEqual(weight, e_weight)

        # Different sorting order
        sort = bf.sort(["Height", "Weight"], [True, False])
        rows = sort.rows
        expected_rows = [
            "Catwoman",
            "Flash",
            "Joker",
            "Wonder Woman",
            "Batman",
            "Superman"]
        for row, e_row in zip(rows, expected_rows):
            self.assertEqual(row, e_row)

        heights = sort["Height"].values
        expected_height = [170, 183, 183, 183, 188, 190.5]
        for height, e_height in zip(heights, expected_height):
            self.assertEqual(height, e_height)

        weights = sort["Weight"].values
        expected_weight = [133, 190, 183, 130, 210, 235]
        for weight, e_weight in zip(weights, expected_weight):
            self.assertEqual(weight, e_weight)

        #                                                  Sort on this column
        #                                                              ⇓
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

        sort = bf.sort("Skin")
        rows = sort.rows
        expected_rows = [
            "Superman",
            "Wonder Woman",
            "Catwoman",
            "Batman",
            "Flash",
            "Joker"]

        for row, e_row in zip(rows, expected_rows):
            self.assertEqual(row, e_row)

        heights = sort["Height"].values
        expected_height = [190.5, 183, 170, 188, 183, 183]
        for height, e_height in zip(heights, expected_height):
            self.assertEqual(height, e_height)

        weights = sort["Weight"].values
        expected_weight = [235, 130, 133, 210, 190, 183]
        for weight, e_weight in zip(weights, expected_weight):
            self.assertEqual(weight, e_weight)

    def test_complex_slicing(self):
        #                                                                              True
        #                                                                               ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  |  ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |  ⇐ This row
        #  ---------------------------------------------------------------------------------

        bf = BatFrame(DatasetTest.dataset_url)
        filtered = bf[bf["Bad"]]
        print(filtered)
        self.assertTupleEqual(filtered.toPandas().shape, (2, 8))
        rowNames = filtered.rows
        expected = ["Catwoman", "Joker"]
        for hero in expected:
            msg = "{} expected to be in the results".format(hero)
            self.assertTrue(hero in rowNames, msg=msg)

        #                                                                           Not True
        #                                                                               ⇓
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
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------

        filtered = bf[~bf["Bad"]]
        print(filtered)
        self.assertTupleEqual(filtered.toPandas().shape, (4, 7))
        rowNames = filtered.rows
        expected = ["Batman", "Wonder Woman", "Flash", "Superman"]
        for hero in expected:
            msg = "{} expected to be in the results".format(hero)
            self.assertTrue(hero in rowNames, msg=msg)

        #                                                                     True  & True
        #                                                                       ⇓      ⇓
        #  ---------------------------------------------------------------------------------
        # |              | Gender | Height | Weight | Eyes  | Hair  | Skin  | Good  | Bad   |
        #  ---------------------------------------------------------------------------------
        # | Batman       | Male   |  188   |  210   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Wonder Woman | Female |  183   |  130   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Flash        | Male   |  183   |  190   | Green | Red   |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Catwoman     | Female |  170   |  133   | Green | Black |       | True  | True  |  ⇐ This row
        #  ---------------------------------------------------------------------------------
        # | Superman     | Male   | 190.5  |  235   | Blue  | Black |       | True  | False |
        #  ---------------------------------------------------------------------------------
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  |
        #  ---------------------------------------------------------------------------------
        filtered = bf[bf["Bad"] & bf["Good"]]
        print(filtered)
        self.assertTupleEqual(filtered.toPandas().shape, (1, 7))
        rowNames = filtered.rows
        expected = ["Catwoman"]
        for hero in expected:
            msg = "{} expected to be in the results".format(hero)
            self.assertTrue(hero in rowNames, msg=msg)

        #                                                                     True  & Not True
        #                                                                       ⇓      ⇓
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
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------
        filtered = bf[bf["Bad"] & ~bf["Good"]]
        print(filtered)
        self.assertTupleEqual(filtered.toPandas().shape, (1, 8))
        rowNames = filtered.rows
        expected = ["Joker"]
        for hero in expected:
            msg = "{} expected to be in the results".format(hero)
            self.assertTrue(hero in rowNames, msg=msg)

        #                                                                     True  |  True
        #                                                                       ⇓      ⇓
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
        filtered = bf[bf["Bad"] | bf["Good"]]
        print(filtered)
        self.assertTupleEqual(filtered.toPandas().shape, (6, 8))
        rowNames = filtered.rows
        expected = [
        "Catwoman",
        "Batman",
        "Wonder Woman",
        "Flash",
        "Superman",
        "Joker"]
        for hero in expected:
            msg = "{} expected to be in the results".format(hero)
            self.assertTrue(hero in rowNames, msg=msg)

        #                                                                     True  | Not True
        #                                                                       ⇓      ⇓
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
        # | Joker        | Male   |  183   |  183   | Green | Green | White | False | True  | ⇐ This row
        #  ---------------------------------------------------------------------------------
        filtered = bf[bf["Bad"] | ~bf["Good"]]
        print(filtered)
        self.assertTupleEqual(filtered.toPandas().shape, (2, 8))
        rowNames = filtered.rows
        expected = ["Catwoman", "Joker"]
        for hero in expected:
            msg = "{} expected to be in the results".format(hero)
            self.assertTrue(hero in rowNames, msg=msg)


if __name__ == '__main__':
    unittest.main(verbosity=2)
