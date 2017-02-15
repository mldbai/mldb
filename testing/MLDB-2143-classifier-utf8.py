# -*- coding: utf-8 -*-

#
# MLDB-2143-classifier-utf8.py
# Mathieu Marquis Bolduc, 2017-02-13
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

# test that classifier.experiment can handle utf8 labels 

import tempfile
import codecs

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2134classiferUtf8Test(MldbUnitTest):  # noqa

    def test_utf8_category(self):
        # create the dataset

        mldb.put('/v1/procedures/import_iris', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "http://public.mldb.ai/iris.data",
                "headers": [ "sepal length", "sepal width", "petal length", "petal width", "class" ],
                "outputDataset": "iris",
                "runOnCreation": True
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
        
        rez = mldb.put("/v1/procedures/cat_weights", {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "cat",
                "modelFileUrlPattern": "file://tmp/iris_utf8.cls",
                "mode": "categorical",
                "inputData": """
                    select 
                        {* EXCLUDING (class)} as features,
                        label
                    from iris_utf8
                """,
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

        runResults = rez.json()["status"]["firstRun"]["status"]["folds"][0]["resultsTest"]["weightedStatistics"]
        self.assertEqual(runResults, {
        "recall": 1.0,
        "support": 72.0,
        "f1Score": 1.0,
        "precision": 1.0,
        "accuracy": 1.0
    })
       

if __name__ == '__main__':
    mldb.run_tests()
