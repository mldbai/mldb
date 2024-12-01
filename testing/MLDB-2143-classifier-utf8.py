# -*- coding: utf-8 -*-

#
# MLDB-2143-classifier-utf8.py
# Mathieu Marquis Bolduc, 2017-02-13
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

# test that classifier.experiment can handle utf8 labels 

import tempfile
import codecs

from mldb import mldb, MldbUnitTest, ResponseException

class MLDB2134classiferUtf8Test(MldbUnitTest):  # noqa

    def test_utf8_category(self):
        # create the dataset

        mldb.put('/v1/procedures/import_iris', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "file://mldb/mldb_test_data/iris.data",
                "headers": [ "sepal length", "sepal width", "petal length", "petal width", "class" ],
                "outputDataset": "iris",
                "runOnCreation": True
            }
        })   

        mldb.put("/v1/procedures/preProcess", {
            "type": "transform",
            "params": {
                "inputData": "SELECT * EXCLUDING (class), class as label FROM iris",
                "outputDataset": { "id": "iris_ascii",
                                   "type": "tabular" },
            }
        })   
        
        mldb.put("/v1/procedures/preProcess", {
            "type": "transform",
            "params": {
                "inputData": "SELECT * EXCLUDING (class), class + '_éç' as label FROM iris",
                "outputDataset": { "id": "iris_utf8",
                                   "type": "tabular" },
            }
        })   

        ds1 = mldb.get("/v1/query", { "q": "select * from iris_ascii limit 10", "format": "table"})
        print("ds1", ds1)
        ds2 = mldb.get("/v1/query", { "q": "select * from iris_utf8 limit 10", "format": "table"})
        print("ds2", ds2)

        def do_query(dataset):            
            return mldb.put("/v1/procedures/cat_weights", {
                    "type": "classifier.experiment",
                    "params": {
                        "experimentName": "cat",
                        "modelFileUrlPattern": "file://tmp/iris_utf8.cls",
                        "mode": "categorical",
                        "inputData": """
                            select 
                                {* EXCLUDING (label)} as features,
                                label
                            from """ + dataset,
                        "algorithm": "dt",
                        "equalizationFactor": 1,
                        "configuration": {
                            "dt": {
                                "type": "decision_tree",
                                "max_depth": 10,
                                "verbosity": 3,
                                "update_alg": "prob"
                            },
                        },
                    }
                })

        rez1 = do_query("iris_ascii")
        rez2 = do_query("iris_utf8")

        runResults1 = rez1.json()["status"]["firstRun"]["status"]["folds"][0]["resultsTest"]["weightedStatistics"]
        runResults2 = rez2.json()["status"]["firstRun"]["status"]["folds"][0]["resultsTest"]["weightedStatistics"]

        self.assertEqual(runResults1, runResults2)

        expected = {
            "recall": 0.9444444444444444,
            "support": 72.0,
            "f1Score": 0.9446548821548821,
            "precision": 0.9537037037037037,
            "accuracy": 0.9645061728395061
        }

        recallError = abs(runResults1["recall"] - expected["recall"])
        supportError = abs(runResults1["support"] - expected["support"])
        f1ScoreError = abs(runResults1["f1Score"] - expected["f1Score"])
        precisionError = abs(runResults1["precision"] - expected["precision"])
        accuracyError = abs(runResults1["accuracy"] - expected["accuracy"])

        self.assertLess(recallError, 0.000000000000001)
        self.assertLess(supportError, 0.000000000000001)
        self.assertLess(f1ScoreError, 0.000000000000001)
        self.assertLess(precisionError, 0.000000000000001)
        self.assertLess(accuracyError, 0.000000000000001)


        #self.assertEqual(runResults1, {
        #    "recall": 0.9444444444444444,
        #    "support": 72.0,
        #    "f1Score": 0.9446548821548821,
        #    "precision": 0.9537037037037037,
        #    "accuracy": 0.9645061728395061
        #})
       

if __name__ == '__main__':
    mldb.run_tests()
