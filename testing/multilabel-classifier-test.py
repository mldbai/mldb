#
# multilabel-classifier-test.py
# Mathieu Marquis Bolduc, March 6th 2017
# this file is part of mldb. copyright 2017 mldb.ai inc. all rights reserved.
#
import datetime, os
from random import random, gauss, seed

mldb = mldb_wrapper.wrap(mldb) # noqa

class MultiLabelClassifierTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(self):
        # Create toy dataset
        for dataset_id in ["toy", "toy2"]:
            dataset_config = {
                'type'    : 'sparse.mutable',
                'id'      : dataset_id
            }

            dataset = mldb.create_dataset(dataset_config)
            now = datetime.datetime.now()

            for i in xrange(5000):
                label = random() < 0.5

                if label:
                    dataset.record_row("u%d" % i, [["feat1", 5, now],
                                                   ["feat2", 0, now],
                                                   ["label0", True, now]])
                else:
                    dataset.record_row("u%d" % i, [["feat1", 0, now],
                                                   ["feat2", 5, now],
                                                   ["label1", True, now]])
            dataset.commit()

        dataset_config = {
                'type'    : 'sparse.mutable',
                'id'      : 'trivial'
            }

        dataset = mldb.create_dataset(dataset_config)
        now = datetime.datetime.now()

        numLabelExample = 5
        for i in xrange(numLabelExample):
            dataset.record_row("u%d" % i, [["feat1", 5, now],
                                           ["feat2", 0, now],
                                           ["label0", True, now]])
            dataset.record_row("u%d" % (i+numLabelExample), [["feat1", 0, now],
                                           ["feat2", 5, now],
                                           ["label1", True, now]])

        dataset.commit()

        dataset_config = {
                'type'    : 'sparse.mutable',
                'id'      : 'trivial2'
            }

        dataset = mldb.create_dataset(dataset_config)
        now = datetime.datetime.now()

        numLabelExample = 20
        for i in xrange(numLabelExample):
            dataset.record_row("u%d" % (1+i*6), [["feat1", 5, now],
                                           ["feat2", 0, now],
                                           ["feat3", 0, now],
                                           ["label0", True, now]])
            dataset.record_row("u%d" % (2+i*6), [["feat1", 0, now],
                                           ["feat2", 5, now],
                                           ["feat3", 0, now],
                                           ["label1", True, now]])
            dataset.record_row("u%d" % (3+i*6), [["feat1", 0, now],
                                           ["feat2", 0, now],
                                           ["feat3", 5, now],
                                           ["label2", True, now]])
            dataset.record_row("u%d" % (4+i*6), [["feat1", 5, now],
                                           ["feat2", 5, now],
                                           ["feat3", 0, now],
                                           ["label0", True, now],
                                           ["label1", True, now]])
            dataset.record_row("u%d" % (5+i*6), [["feat1", 5, now],
                                           ["feat2", 0, now],
                                           ["feat3", 5, now],
                                           ["label0", True, now],
                                           ["label2", True, now]])
            dataset.record_row("u%d" % (6+i*6), [["feat1", 0, now],
                                           ["feat2", 5, now],
                                           ["feat3", 5, now],
                                           ["label1", True, now],
                                           ["label2", True, now]])

        dataset.commit()

        dataset_config = {
                'type'    : 'sparse.mutable',
                'id'      : 'categoricalds'
            }

        dataset = mldb.create_dataset(dataset_config)
        now = datetime.datetime.now()

        seed(123456)
        numLabelExample = 20
        for i in xrange(numLabelExample):
            dataset.record_row("u%d" % (1+i*3), [["feat1", gauss(5,5), now],
                                           ["feat2", gauss(3,2), now],
                                           ["feat3", gauss(3,2), now],
                                           ["label", "banane", now]])
            dataset.record_row("u%d" % (2+i*3), [["feat1", gauss(3,2), now],
                                           ["feat2", gauss(5,5), now],
                                           ["feat3", gauss(3,2), now],
                                           ["label", "pomme", now]])
            dataset.record_row("u%d" % (3+i*3), [["feat1", gauss(3,2), now],
                                           ["feat2", gauss(3,2), now],
                                           ["feat3", gauss(5,5), now],
                                           ["label", "orange", now]])

        dataset.commit()

    def test_random_simple(self):

        conf = {
            "type": "classifier.train",
            "params": {
                "trainingData": "select {* EXCLUDING(label0, label1)} as features, {label0, label1} as label from toy",                
                "modelFileUrl": "file://build/x86_64/tmp/multilabel1-$runid.cls",
                "algorithm": "dt",
                "mode": "multilabel",
                "multilabelStrategy": "random",
                "functionName" : "classifyMe",
                "configuration": {                   
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 0,
                        "update_alg": "gentle",
                        "random_feature_propn": 1
                    }
                },
            }
        }

        mldb.put("/v1/procedures/multilabel_train", conf)

        res = mldb.query("SELECT classifyMe({features : {5 as feat1, 0 as feat2}}) as *")
        self.assertTableResultEquals(res, [
            [
                "_rowName",
                "scores.\"\"\"label0\"\"\"",
                "scores.\"\"\"label1\"\"\""
            ],
            [
                "result",
                1,
                -1
            ]
        ])

        conf = {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "my_test_exp",
                "inputData": "select {* EXCLUDING(label0, label1)} as features, {label0, label1} as label from toy",
                "testingDataOverride": "select {* EXCLUDING(label0, label1)} as features, {label0, label1} as label from toy",
                "datasetFolds" : [
                    {
                        "trainingWhere": "rowHash() % 10 < 7",
                        "testingWhere": "rowHash() % 10 >= 7",
                    }],
                "modelFileUrlPattern": "file://build/x86_64/tmp/multilabel1-$runid.cls",
                "algorithm": "dt",
                "equalizationFactor": 0.5,
                "mode": "multilabel",
                "multilabelStrategy": "random",
                "recallOverN" : [1],
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 0,
                        "update_alg": "gentle",
                        "random_feature_propn": 1
                    }                    
                },
                "outputAccuracyDataset": False
            }
        }

        rez = mldb.put("/v1/procedures/rocket_science", conf)
        rez = mldb.post("/v1/procedures/rocket_science/runs")
        js_rez = rez.json()

        self.assertEqual(
            js_rez["status"]["folds"][0]["resultsTest"]["weightedStatistics"]["recallOverTopN"][0], 1.0)

    def test_decompose_simple(self):

        conf = {
            "type": "classifier.train",
            "params": {
                "trainingData": "select {* EXCLUDING(label0, label1)} as features, {label0, label1} as label from toy",
                "modelFileUrl": "file://build/x86_64/tmp/multilabel1-$runid.cls",
                "algorithm": "dt",
                "mode": "multilabel",
                "multilabelStrategy": "decompose",
                "functionName" : "classifyMe",
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 0,
                        "update_alg": "gentle",
                        "random_feature_propn": 1
                    }
                },
            }
        }

        mldb.put("/v1/procedures/multilabel_train", conf)

        res = mldb.query("SELECT classifyMe({features : {5 as feat1, 0 as feat2}}) as *")
        self.assertTableResultEquals(res, [
            [
                "_rowName",
                "scores.\"\"\"label0\"\"\"",
                "scores.\"\"\"label1\"\"\""
            ],
            [
                "result",
                1,
                -1
            ]
        ])

        conf = {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "my_test_exp",
                "inputData": "select {* EXCLUDING(label0, label1)} as features, {label0, label1} as label from toy",
                "testingDataOverride": "select {* EXCLUDING(label0, label1)} as features, {label0, label1} as label from toy",
                "datasetFolds" : [
                    {
                        "trainingWhere": "rowHash() % 10 < 7",
                        "testingWhere": "rowHash() % 10 >= 7",
                    }],
                "modelFileUrlPattern": "file://build/x86_64/tmp/multilabel1-$runid.cls",
                "algorithm": "dt",
                "equalizationFactor": 0.5,
                "mode": "multilabel",
                "multilabelStrategy": "decompose",
                "recallOverN" : [1],
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 0,
                        "update_alg": "gentle",
                        "random_feature_propn": 1
                    }
                },
                "outputAccuracyDataset": False
            }
        }

        rez = mldb.put("/v1/procedures/rocket_science", conf)
        rez = mldb.post("/v1/procedures/rocket_science/runs")
        js_rez = rez.json()

        self.assertEqual(
            js_rez["status"]["folds"][0]["resultsTest"]["weightedStatistics"]["recallOverTopN"][0], 1.0)

    def test_onevsall_simple(self):
        conf = {
            "type": "classifier.train",
            "params": {
                "trainingData": "select {* EXCLUDING(label0, label1)} as features, {label0, label1} as label from trivial",
                "modelFileUrl": "file://build/x86_64/tmp/multilabel1-$runid.cls",
                "algorithm": "dt",
                "mode": "multilabel",
                "multilabelStrategy": "one-vs-all",
                "functionName" : "classifyMe",
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 3,
                        "update_alg": "gentle",
                        "random_feature_propn": 1
                    }
                },
            }
        }

        mldb.put("/v1/procedures/multilabel_train", conf)

        res = mldb.query("SELECT classifyMe({features : {5 as feat1, 0 as feat2}}) as *")
        self.assertTableResultEquals(res, [
            [
                "_rowName",
                "scores.\"\"\"label0\"\"\"",
                "scores.\"\"\"label1\"\"\""
            ],
            [
                "result",
                0.9999726414680481,
                2.73847472271882e-05
            ]
        ])

    def test_onevsall_combinaison(self):
        conf = {
            "type": "classifier.train",
            "params": {
                "trainingData": "select {* EXCLUDING(label0, label1, label2)} as features, {label0, label1, label2} as label from trivial2",
                "modelFileUrl": "file://build/x86_64/tmp/multilabel1-$runid.cls",
                "algorithm": "dt",
                "mode": "multilabel",
                "multilabelStrategy": "one-vs-all",
                "functionName" : "classifyMe",
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 3,
                        "update_alg": "gentle",
                        "random_feature_propn": 1
                    }
                },
            }
        }

        mldb.put("/v1/procedures/multilabel_train", conf)

        res = mldb.query("SELECT classifyMe({features : {5 as feat1, 0 as feat2, 5 as feat3}}) as *")
        self.assertTableResultEquals(res, [
            [
                "_rowName",
                "scores.\"\"\"label0\"\"\"",
                "scores.\"\"\"label1\"\"\"",
                "scores.\"\"\"label2\"\"\""
            ],
            [
                "result",
                0.9999789595603943,
                3.6201177863404155e-05,
                0.9999789595603943
            ]
        ])

    def test_accuracy_multilabel (self):
        conf_classifier = {
            "type": "classifier.train",
            "params": {
                "trainingData": "select {* EXCLUDING(label0, label1, label2)} as features, {label0, label1, label2} as label from trivial2",
                "modelFileUrl": "file://build/x86_64/tmp/multilabel1-$runid.cls",
                "algorithm": "dt",
                "mode": "multilabel",
                "multilabelStrategy": "one-vs-all",
                "functionName" : "classifyMe",
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 3,
                        "update_alg": "gentle",
                        "random_feature_propn": 1
                    }
                },
            }
        }

        mldb.put("/v1/procedures/multilabel_train", conf_classifier)

        accuracyConf = {
            "type": "classifier.test",
            "params": {
                "testingData": "select classifyMe({{* EXCLUDING(label0, label1, label2)} as features}) as score, {label0, label1, label2} as label from trivial2",
                "mode" : "multilabel",
                "recallOverN" : [1, 2],
                "runOnCreation": True
            }
        }

        res = mldb.put("/v1/procedures/multilabel_accuracy", accuracyConf);

        self.assertEquals(res.json()["status"]["firstRun"]["status"], {
                "weightedStatistics": {
                    "coverageError": 1.333333333333333,
                    "recallOverTopN": [
                        0.6666666666666666,
                        1.0
                    ]
                },
                "recallOverN": [
                    1,
                    2
                ],
                "labelStatistics": {
                    "label0": {
                        "recallOverTopN": [
                            0.6666666666666666,
                            1.0
                        ]
                    },
                    "label1": {
                        "recallOverTopN": [
                            0.6666666666666666,
                            1.0
                        ]
                    },
                    "label2": {
                        "recallOverTopN": [
                            0.6666666666666666,
                            1.0
                        ]
                    }
                }
            })

    def test_precision_over_n_categorical (self):
        conf_classifier = {
            "type": "classifier.train",
            "params": {
                "trainingData": "select {* EXCLUDING(label)} as features, label from categoricalds",
                "modelFileUrl": "file://build/x86_64/tmp/categorical1-$runid.cls",
                "algorithm": "dt",
                "mode": "categorical",
                "functionName" : "classifyMe",
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 2,
                        "verbosity": 3,
                        "update_alg": "gentle",
                        "random_feature_propn": 1
                    }
                },
            }
        }

        mldb.put("/v1/procedures/categorical_train", conf_classifier)

        accuracyConf = {
            "type": "classifier.test",
            "params": {
                "testingData": "select classifyMe({{* EXCLUDING(label)} as features}) as score, label from categoricalds",
                "mode" : "categorical",
                "recallOverN" : [2,3],
                "runOnCreation": True
            }
        }

        res = mldb.put("/v1/procedures/categorical_accuracy", accuracyConf);

        self.assertEquals(res.json()["status"]["firstRun"]["status"], {
                "weightedStatistics": {
                    "recall": 0.65,
                    "support": 60.0,
                    "precision": 0.8292682926829269,
                    "recallOverTopN": [
                        0.8333333333333334,
                        1.0
                    ],
                    "f1Score": 0.6476980089190377,
                    "accuracy": 0.7666666666666667
                },
                "labelStatistics": {
                    "orange": {
                        "recall": 0.5,
                        "support": 20.0,
                        "precision": 1.0,
                        "recallOverTopN": [
                            0.5,
                            1.0
                        ],
                        "f1Score": 0.6666666666666666,
                        "accuracy": 0.8333333333333334
                    },
                    "banane": {
                        "recall": 0.45,
                        "support": 20.0,
                        "precision": 1.0,
                        "recallOverTopN": [
                            1.0,
                            1.0
                        ],
                        "f1Score": 0.6206896551724138,
                        "accuracy": 0.8166666666666667
                    },
                    "pomme": {
                        "recall": 1.0,
                        "support": 20.0,
                        "precision": 0.4878048780487805,
                        "recallOverTopN": [
                            1.0,
                            1.0
                        ],
                        "f1Score": 0.6557377049180327,
                        "accuracy": 0.65
                    }
                },
                "recallOverN": [
                    2,
                    3
                ],
                "confusionMatrix": [
                    {
                        "count": 9.0,
                        "actual": "banane",
                        "predicted": "banane"
                    },
                    {
                        "count": 11.0,
                        "actual": "banane",
                        "predicted": "pomme"
                    },
                    {
                        "count": 10.0,
                        "actual": "orange",
                        "predicted": "orange"
                    },
                    {
                        "count": 10.0,
                        "actual": "orange",
                        "predicted": "pomme"
                    },
                    {
                        "count": 20.0,
                        "actual": "pomme",
                        "predicted": "pomme"
                    }
                ]
            })       

if __name__ == '__main__':
    mldb.run_tests()