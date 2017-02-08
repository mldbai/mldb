# -*- coding: utf-8 -*-

# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

# Copyright (c) 2015 mldb.ai inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Date:               2015-02-26 13:47:09
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-03-13 15:57:56
# @File Name:          mldb_dataset_test_py.py

import unittest
import csv
import requests
import os
import json

from mldb_py_runner.mldb_py_runner import MldbRunner


class DatasetTest(unittest.TestCase):

    def setUp(self):
        self.mldb = MldbRunner()
        self.port = self.mldb.port
        self.url = "http://localhost:%d/v1" % self.port
        self.writeDatasetToDisk()
        self.loadDatasetToMLDB()

    def tearDown(self):
        if os.path.exists('mldb_dataset_test_py.beh.gz'):
            os.remove('mldb_dataset_test_py.beh.gz')
        if os.path.exists('mldb_dataset_test_py.csv'):
            os.remove('mldb_dataset_test_py.csv')

    def writeDatasetToDisk(self):
        rows = [["35753787",
                "41365040@N00",
                "gizzypooh",
                "2005-08-05 11:58:57.0",
                "1124596353",
                "Canon+EOS+DIGITAL+REBEL+XT",
                "IMG_0976",
                "hubby+%26+wifey",
                "2005,europe,london",
                "",
                "",
                "",
                "",
                "http://www.flickr.com/photos/41365040@N00/35753787/",
                "http://farm1.staticflickr.com/32/35753787_a592e00d5f.jpg",
                "Attribution-NonCommercial-NoDerivs License",
                "http://creativecommons.org/licenses/by-nc-nd/2.0/",
                "32",
                "1",
                "a592e00d5f",
                "a592e00d5f",
                "jpg",
                "0"],
                ["2285954181",
                "21173961@N07",
                "piX1966",
                "2008-02-23 14:27:24.0",
                "1203796507",
                "OLYMPUS+IMAGING+CORP.+E-410",
                "Outer+wall",
                "",
                "architecture,battle+abbey,east+sussex,hastings,heritage,texture,walls",
                "",
                "",
                "",
                "",
                "http://www.flickr.com/photos/21173961@N07/2285954181/",
                "http://farm3.staticflickr.com/2336/2285954181_17191638fa.jpg",
                "Attribution-NoDerivs License",
                "http://creativecommons.org/licenses/by-nd/2.0/",
                "2336",
                "3",
                "17191638fa",
                "370b04a8ce",
                "jpg",
                "0"]]


        f = open('mldb_dataset_test_py.csv', 'w')
        csv_writer = csv.writer(f, delimiter='\t')
        csv_writer.writerows(rows)
        f.close()
        self.mldb_dataset_test_py = 'mldb_dataset_test_py.csv'

    def loadDatasetToMLDB(self):
        dataset_name = 'mldb_dataset_test_py'

        pythonScript = {
        "source":  """
import datetime
import random

data = {
        "type": "sparse.mutable",
        "id": '""" + dataset_name + """'
        }
dataset = mldb.create_dataset(data)

col_names = ["Photo/video identifier", "User_NSID",
"User nickname", "Date taken", "Date uploaded", "Capture device",
"Title", "Description", "User tags (comma-separated)",
"Machine tags (comma-separated)", "Longitude", "Latitude", "Accuracy",
"Photo/video page URL", "Photo/video download URL", "License name",
"License URL", "Photo/video server identifier", "Photo/video farm identifier",
"Photo/video secret", "Photo/video secret original",
"Photo/video extension original", "Photos/video marker"]

with open('mldb_dataset_test_py.csv') as f:
    for line in f:
        split_line = line.split("\t")
        if len(split_line) != len(col_names):

            continue
        row = split_line[0]
        try:
            # Just checking if datetime format is valid
            ts = datetime.datetime.strptime(split_line[3],
                "%Y-%m-%d %H:%M:%S.%f")
            if ts.year < 1400 or ts.year > 10000:
                ts = datetime.datetime.now()

            # send in half the events with a ts in string format to make sure
            # we can handle both python dates and string iso representations
            if random.random() < 0.5:
                ts = ts.isoformat()

        except:
            ts = datetime.datetime.now()

        cols = []

        for i in [8, 13, 14, -1]:
            s = split_line[i]
            if s == '' or s is None or s == -1 or s == '-1':
                continue
            cols.append([col_names[i], str(s).strip(), ts])

        data = {"rowName": "r" + str(row), "columns": cols}
        dataset.record_row(row, cols)
dataset.commit()
"""
    }

        requests.post(self.url + "/types/plugins/python/routes/run",
                      json.dumps(pythonScript))


    def test_datasetIsInRunner(self):
        response = requests.get(self.url + "/datasets/mldb_dataset_test_py")
        datasets = json.loads(response.content)
        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(datasets['status']['rowCount'], 2)

if __name__ == '__main__':
    unittest.main(verbosity=2)
