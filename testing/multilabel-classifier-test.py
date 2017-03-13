#
# multilabel-classifier-test.py
# Mathieu Marquis Bolduc, March 6th 2017
# this file is part of mldb. copyright 2017 mldb.ai inc. all rights reserved.
#
import datetime, os
from random import random, gauss

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

        #numLabelExample = 5
        #for i in xrange(numLabelExample):
        dataset.record_row("u%d" % 1, [["feat1", 5, now],
                                       ["feat2", 0, now],
                                       ["feat3", 0, now],
                                       ["label0", True, now]])
        dataset.record_row("u%d" % 2, [["feat1", 0, now],
                                       ["feat2", 5, now],
                                       ["feat3", 0, now],
                                       ["label1", True, now]])
        dataset.record_row("u%d" % 3, [["feat1", 0, now],
                                       ["feat2", 0, now],
                                       ["feat3", 5, now],
                                       ["label2", True, now]])
        dataset.record_row("u%d" % 4, [["feat1", 5, now],
                                       ["feat2", 5, now],
                                       ["feat3", 0, now],
                                       ["label0", True, now],
                                       ["label1", True, now]])
        dataset.record_row("u%d" % 5, [["feat1", 5, now],
                                       ["feat2", 0, now],
                                       ["feat3", 5, now],
                                       ["label0", True, now],
                                       ["label2", True, now]])
        dataset.record_row("u%d" % 6, [["feat1", 0, now],
                                       ["feat2", 5, now],
                                       ["feat3", 5, now],
                                       ["label1", True, now],
                                       ["label2", True, now]])

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

        mldb.log(js_rez)

        self.assertEqual(
            js_rez["status"]["folds"][0]["resultsTest"]["weightedStatistics"]["precision"], 1.0)

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

        mldb.log(js_rez)

        self.assertEqual(
            js_rez["status"]["folds"][0]["resultsTest"]["weightedStatistics"]["precision"], 1.0)

    
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

        mldb.log(mldb.query("SELECT label0, label1, count(*) FROM trivial GROUP BY label0, label1"));

        res = mldb.query("SELECT classifyMe({features : {5 as feat1, 0 as feat2}}) as *")
        mldb.log(res)
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

        mldb.log(mldb.query("SELECT label0, label1, label2, count(*) FROM trivial2 GROUP BY label0, label1, label2"));

        res = mldb.query("SELECT classifyMe({features : {5 as feat1, 0 as feat2, 5 as feat3}}) as *")
        mldb.log(res)
        self.assertTableResultEquals(res, [
            [
                "_rowName",
                "scores.\"\"\"label0\"\"\"",
                "scores.\"\"\"label1\"\"\"",
                "scores.\"\"\"label2\"\"\""
            ],
            [
                "result",
                1,
                -1,
                1
            ]
        ])

if __name__ == '__main__':
    mldb.run_tests()