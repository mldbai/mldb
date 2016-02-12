#
# MLDB-256_accuracy_accepts_all_cls_modes.py
# Francois Maillet, 11 fevrier 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb256Test(MldbUnitTest):  

    @classmethod
    def setUpClass(self):
        # create a boolean classification dataset
        ds = mldb.create_dataset({ "id": "boolean", "type": "sparse.mutable" })
        ds.record_row("a",[["label", 1, 0], ["x", 0, 0]])
        ds.record_row("b",[["label", 1, 0], ["x", 0, 0]])
        ds.record_row("c",[["label", 1, 0], ["x", 0, 0]])
        ds.record_row("d",[["label", 1, 0], ["x", 0, 0]])
        ds.record_row("e",[["label", 1, 0], ["x", 0.6, 0]])
        ds.record_row("f",[["label", 0, 0], ["x", 0.4, 0]])
        ds.record_row("g",[["label", 0, 0], ["x", 1, 0]])
        ds.record_row("h",[["label", 0, 0], ["x", 1, 0]])
        ds.record_row("i",[["label", 0, 0], ["x", 1, 0]])
        ds.record_row("j",[["label", 0, 0], ["x", 1, 0]])
        ds.commit()
        
        #  toy multi class reprenseting the output of a classifier
        # this is used to test the multi-class metrics
        ds = mldb.create_dataset({ "id": "toy_categorical", "type": "sparse.mutable" })
        ds.record_row("a",[["label", 0, 0], ["0", 1, 0], ["1", 0, 0], ["2", 0, 0]])
        ds.record_row("b",[["label", 1, 0], ["0", 1, 0], ["1", 0, 0], ["2", 0, 0]])
        ds.record_row("c",[["label", 2, 0], ["0", 0, 0], ["1", 0, 0], ["2", 1, 0]])
        ds.record_row("d",[["label", 2, 0], ["0", 0, 0], ["1", 0, 0], ["2", 1, 0]])
        ds.record_row("e",[["label", 0, 0], ["0", 1, 0], ["1", 0, 0], ["2", 0, 0]])
        ds.commit()
       
        # create a multi-class classification dataset on which we will train
        # this is used to test if we can train using the classifier.expr procesude
        ds = mldb.create_dataset({ "id": "categorical", "type": "sparse.mutable" })
        ds.record_row("a",[["label", "x", 0], ["col", 0, 0], ["col2", 80, 0]])
        ds.record_row("b",[["label", "x", 0], ["col", 0.1, 0], ["col2", 75, 0]])
        ds.record_row("c",[["label", "x", 0], ["col", 0.05, 0], ["col2", 60, 0]])
        ds.record_row("c2",[["label", "x", 0], ["col", 0.15, 0], ["col2", 98, 0]])
        ds.record_row("d",[["label", "y", 0], ["col", 0.45, 0], ["col2", 0.4, 0]])
        ds.record_row("e",[["label", "y", 0], ["col", 0.6, 0], ["col2", 15, 0]])
        ds.record_row("f",[["label", "y", 0], ["col", 0.4, 0], ["col2", 80, 0]])
        ds.record_row("f2",[["label", "y", 0], ["col", 0.5, 0], ["col2", 10, 0]])
        ds.record_row("g",[["label", "z", 0], ["col", 0.98, 0], ["col2", 50, 0]])
        ds.record_row("h",[["label", "z", 0], ["col", 1.25, 0], ["col2", 55, 0]])
        ds.record_row("i",[["label", "z", 0], ["col", 0.80, 0], ["col2", 56, 0]])
        ds.record_row("i2",[["label", "z", 0], ["col", 0.90, 0], ["col2", 58, 0]])
        ds.commit()

    def test_toy_categorical_eval_works(self):

        # TODO test case selecting columns using number
        #mldb.log(mldb.query("SELECT {* EXCLUDING(label)} as score, label as label from toy_categorical"))

        rez = mldb.put("/v1/procedures/toy_eval", {
            "type": "classifier.test",
            "params": {
                "mode": "categorical",
                "testingData": """

                    SELECT {* EXCLUDING(label)} as score, label as label from toy_categorical

                """,
                "runOnCreation": True
            }
        })

        jsRez = rez.json()
        mldb.log(rez.json())

        goodLabelStatistics = {
                    "1": {
                        "f1_score": 0.0,
                        "recall": 0.0,
                        "support": 1,
                        "precision": 0.0
                    },
                    "0": {
                        "f1_score": 0.8000000143051146,
                        "recall": 1.0,
                        "support": 2,
                        "precision": 0.6666666865348816
                    },
                    "2": {
                        "f1_score": 1.0,
                        "recall": 1.0,
                        "support": 2,
                        "precision": 1.0
                    }
                }

        self.assertEqual(jsRez["status"]["firstRun"]["status"]["label_statistics"],
                         goodLabelStatistics)

        total_f1 = 0
        total_recall = 0
        total_support = 0
        total_precision = 0
        for val in goodLabelStatistics.itervalues():
            total_f1 += val["f1_score"] * val["support"]
            total_recall += val["recall"] * val["support"]
            total_precision += val["precision"] * val["support"]
            total_support += val["support"]

        self.assertEqual(jsRez["status"]["firstRun"]["status"]["weighted_statistics"],
                {
                    "f1_score": total_f1 / total_support,
                    "recall": total_recall / total_support,
                    "support": total_support,
                    "precision": total_precision / total_support
                })


    def test_categorical_cls_works(self):
        rez = mldb.put("/v1/procedures/categorical_cls", {
            "type": "classifier.experiment",
            "params": {
                "trainingData": "select {col*} as features, label as label from categorical",
                "experimentName": "categorical_exp",
                "keepArtifacts": True,
                "modelFileUrlPattern": "file://temp/mldb-256_cat.cls",
                "algorithm": "glz",
                "configuration": {
                    "glz": {
                        "type": "glz",
                        "verbosity": 3,
                        "normalize": False,
                        "link": "linear",
                        "ridge_regression": True
                    }
                },
                "datasetFolds": [
                    {
                        "training_where": "rowHash() % 2 = 1",
                        "testing_where": "rowHash() % 2 = 0",
                    }],
                "mode": "categorical",
                "outputAccuracyDataset": True,
                "runOnCreation": True
            }
        })

        jsRez = rez.json()
        mldb.log(jsRez["status"]["firstRun"]["status"]["folds"][0]["results"]["confusion_matrix"])
        self.assertEqual(jsRez["status"]["firstRun"]["status"]["folds"][0]["results"]["confusion_matrix"],
                {
                    "y": { "y": 1 },
                    "x": { "x": 3 },
                    "z": { "z": 1 }
                })


        # Check the accuracy dataset
        self.assertEqual(len(mldb.query("select * from categorical_exp_results_0")), 6)


mldb.run_tests()


