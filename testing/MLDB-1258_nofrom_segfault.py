#
# MLDB-1258_nofrom_segfault.py
# 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
import unittest

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


class NoFromSegfaultTest(unittest.TestCase):

    def test_1(self):
        conf = {
            "type": "classifier.train",
            "params": {
                "trainingData": """
                    select {* EXCLUDING(quality)} as features, quality as label
                """,
                "modelFileUrl": "file://my_model.cls",
                "algorithm": "glz",
                "equalizationFactor": 0.5,
                "mode": "regression",
                "functionName": "myScorer",
                "runOnCreation": True
            }
        }
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/procedures/trainer", conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_2(self):
        conf = {
            "type": "probabilizer.train",
            "params": {
                "trainingData": """
                    select {* EXCLUDING(quality)} as features, quality as label
                """,
                "modelFileUrl": "file://my_model.cls",
                "runOnCreation": True
            }
        }
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/procedures/trainer2", conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_3(self):
        conf = {
            "type": "classifier.test",
            "params": {
                "testingData": """
                    select {* EXCLUDING(quality)} as score, quality as label
                """,
                "runOnCreation": True
            }
        }
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/procedures/trainer3", conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_4(self):
        conf = {
            "type": "tsne.train",
            "params": {
                "trainingData": """
                    select {* EXCLUDING(quality)} as features, quality as label
                """,
                "runOnCreation": True
            }
        }
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/procedures/trainer3", conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_5(self):
        conf = {
            "type": "kmeans.train",
            "params": {
                "trainingData": """
                    select {* EXCLUDING(quality)} as features, quality as label
                """,
                "runOnCreation": True
            }
        }
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/procedures/trainer3", conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_6(self):
        conf = {
            "type": "svm.train",
            "params": {
                "trainingData": """
                    select {* EXCLUDING(quality)} as features, quality as label
                """,
                "runOnCreation": True
            }
        }
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/procedures/trainer3", conf)
        self.assertEqual(re.exception.response.status_code, 400)

if __name__ == '__main__':
    mldb.run_tests()
