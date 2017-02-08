#
# MLDB-1258_nofrom_segfault.py
# 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


class NoFromSegfaultTest(unittest.TestCase):


    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "dataset", "type": "sparse.mutable" })
        ds.record_row("a",[["label", 1, 0]])
        ds.commit()

    def test_1(self):
        conf = {
            "type": "classifier.train",
            "params": {
                "trainingData": """
                    select {* EXCLUDING(quality)} as features, quality as label
                """,
                "modelFileUrl": "file://tmp/my_model.cls",
                "algorithm": "glz",
                "equalizationFactor": 0.5,
                "mode": "regression",
                "functionName": "myScorer",
                "runOnCreation": True
            }
        }
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
            mldb.put("/v1/procedures/trainer", conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_2(self):
        conf = {
            "type": "probabilizer.train",
            "params": {
                "trainingData": """
                    select {* EXCLUDING(quality)} as features, quality as label
                """,
                "modelFileUrl": "file://tmp/my_model.cls",
                "runOnCreation": True
            }
        }
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
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
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
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
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
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
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
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
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
            mldb.put("/v1/procedures/trainer3", conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_7(self):
        conf = {
            "type": "bucketize",
            "params": {
                "inputData": """
                    select 1
                """,
                "runOnCreation": True
            }
        }
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
            mldb.put("/v1/procedures/trainer3", conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_8(self):
        conf = {
            "type": "export.csv",
            "params": {
                "exportData": """
                    select 1
                """,
                "runOnCreation": True
            }
        }
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
            mldb.put("/v1/procedures/trainer3", conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_9(self):
        conf = {
            "type": "ranking",
            "params": {
                "inputData": """
                    select 1
                """,
                "runOnCreation": True
            }
        }
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
            mldb.put("/v1/procedures/trainer3", conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_10(self):
        conf = {
            "type": "statsTable.train",
            "params": {
                "trainingData": """
                    select {* EXCLUDING(quality)} as features, quality as label
                """,
                "runOnCreation": True
            }
        }
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
            mldb.put("/v1/procedures/trainer3", conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_11(self):
        conf = {
            "type": "statsTable.bagOfWords.train",
            "params": {
                "trainingData": """
                    select {* EXCLUDING(quality)} as features, quality as label
                """,
                "runOnCreation": True
            }
        }
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
            mldb.put("/v1/procedures/trainer3", conf)
        self.assertEqual(re.exception.response.status_code, 400)

    def test_12(self):
        conf = {
            "type": "svd.train",
            "params": {
                "trainingData": """
                    select {* EXCLUDING(quality)} as features, quality as label
                """,
                "runOnCreation": True
            }
        }
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
            mldb.put("/v1/procedures/trainer3", conf)
        self.assertEqual(re.exception.response.status_code, 400)


    def test_13(self):
        conf = {
            "type": "tfidf.train",
            "params": {
                "trainingData": """
                    select {* EXCLUDING(quality)} as features, quality as label
                """,
                "runOnCreation": True
            }
        }
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 'must contain a FROM clause') as re:
            mldb.put("/v1/procedures/trainer3", conf)
        self.assertEqual(re.exception.response.status_code, 400)


    def test_MLDB_1386(self):
        conf = {
            "type": "classifier.train",
            "params": {
                "trainingData": """
                    select {donotexist} as features, label from dataset
                """,
                "modelFileUrl": "file://tmp/my_model.cls",
                "algorithm": "glz",
                "configuration": {
                    "glz": {
                        "type": "glz",
                        "verbosity": 3,
                        "normalize": False,
                        "link_function": 'linear',
                        "regularization": 'none'
                    }
                },
                "mode": "regression",
                "functionName": "myScorer1386",
                "runOnCreation": True
            }
        }

        mldb.log(mldb.put("/v1/procedures/trainer1386", conf).json())

if __name__ == '__main__':
    mldb.run_tests()
