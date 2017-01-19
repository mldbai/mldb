#
# MLDB-1766_dt_categorical_iris.py
# Francois Maillet, 2016-06-30
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

from datetime import datetime
from random import random, gauss

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1766DtCategoricalIris(MldbUnitTest):  # noqa
    @classmethod
    def setUpClass(cls):
        mldb.put('/v1/procedures/import_iris', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "http://public.mldb.ai/iris.data",
                "headers": [ "sepal length", "sepal width", "petal length", "petal width", "class" ],
                "outputDataset": "iris",
                "runOnCreation": True
            }
        })

        ## create unbalanced dataset
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : "cat_weights"
        }

        dataset = mldb.create_dataset(dataset_config)
        now = datetime.now()

        for label in ["a", "b"]:
            for i in xrange(5000):
                dataset.record_row("u%d-%s" % (i, label), 
                        [["feat1", gauss(5 if label == "a" else 15, 3), now],
                         ["feat2", gauss(-5 if label == "a"  else 10, 10), now],
                         ["feat3", gauss(0, 10), now],
                         ["label", label, now]])

        for i in xrange(500):
            dataset.record_row("u%d-c" % i, 
                        [["feat1", gauss(10, 5), now],
                         ["feat2", gauss(0, 10), now],
                         ["feat3", gauss(5, 10), now],
                         ["label", "c", now]])

        dataset.commit()


    def test_not_perfect_performance(self):
        mldb.put('/v1/procedures/iris_train_classifier', {
            'type' : 'classifier.train',
            'params' : {
                'trainingData' : """
                    select 
                        {* EXCLUDING(class)} as features, 
                        class as label 
                    from iris 
                    where rowHash() % 5 = 0
                """,
                "algorithm": "dt",
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 3,
                        "update_alg": "prob"
                    },
                },
                "modelFileUrl": "file://tmp/iris.cls",
                "mode": "categorical",
                "functionName": "iris_classify",
                "runOnCreation": True
            }
        })

        rez = mldb.put('/v1/procedures/iris_test_classifier', {
            'type' : 'classifier.test',
            'params' : {
                'testingData' : """
                    select 
                        iris_classify({
                            features: {* EXCLUDING(class)}
                        }) as score,
                        class as label 
                    from iris 
                    where rowHash() % 5 != 0
                """,
                "mode": "categorical",
                "runOnCreation": True
            }
        })

        runResults = rez.json()["status"]["firstRun"]["status"]

        self.assertLess(runResults["labelStatistics"]["Iris-virginica"]["recall"], 0.98)

    
    def test_boolean_unbalanced(self):
        ## create new dataset without label = b
        mldb.put("/v1/procedures/bumblebee", {
            "type": "transform",
            "params": {
                "inputData": """
                    select feat*, label ='a' as label 
                    from cat_weights
                    where label != 'b'
                """,
                "outputDataset": "boolean_unbalanced",
                "runOnCreation": True
            }
        })

        rez = mldb.put("/v1/procedures/cat_weights", {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "cat_weights",
                "mode": "boolean",
                "inputData": """
                    select 
                        {feat*} as features,
                        label
                    from boolean_unbalanced
                """,
                "algorithm": "dt",
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 3,
                        "update_alg": "prob"
                    },
                },
                "modelFileUrlPattern": "file://tmp/cat_weights.cls",
                "runOnCreation": True
            }
        })

        runResults = rez.json()["status"]["firstRun"]["status"]["folds"][0]["resultsTest"]
        self.assertGreater(runResults["auc"], 0.68)

    def test_categorical_unbalanced(self):
        rez = mldb.put("/v1/procedures/cat_weights", {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "cat_weights",
                "mode": "categorical",
                "inputData": """
                    select 
                        {feat*} as features,
                        label
                    from cat_weights
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
                "modelFileUrlPattern": "file://tmp/cat_weights.cls",
                "runOnCreation": True
            }
        })

        runResults = rez.json()["status"]["firstRun"]["status"]["folds"][0]["resultsTest"]
        self.assertGreater(runResults["labelStatistics"]["c"]["accuracy"], 0.2)



if __name__ == '__main__':
    mldb.run_tests()
