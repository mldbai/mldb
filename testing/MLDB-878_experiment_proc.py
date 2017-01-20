#
# MLDB-878_experiment_proc.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
import datetime, os
from random import random, gauss

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb878Test(MldbUnitTest):  # noqa

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
                label = random() < 0.2
                dataset.record_row("u%d" % i, [["feat1", gauss(5 if label else 15, 3), now],
                                               ["feat2", gauss(-5 if label else 10, 10), now],
                                               ["label", label, now]])

            dataset.commit()

    def test_all(self):
        conf = {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "my_test_exp",
                "inputData": "select {* EXCLUDING(label)} as features, label from toy",
                "testingDataOverride": "select {* EXCLUDING(label)} as features, label from toy",
                "datasetFolds" : [
                    {
                        "trainingWhere": "rowHash() % 5 != 3",
                        "testingWhere": "rowHash() % 5 = 3",
                    },
                    {
                        "trainingWhere": "rowHash() % 5 != 2",
                        "testingWhere": "rowHash() % 5 = 2",
                    }],
                "modelFileUrlPattern": "file://build/x86_64/tmp/bouya-$runid.cls",
                "algorithm": "glz",
                "equalizationFactor": 0.5,
                "mode": "boolean",
                "configuration": {
                    "glz": {
                        "type": "glz",
                        "verbosity": 3,
                        "normalize": False,
                        "regularization": 'l2'
                    }
                },
                "outputAccuracyDataset": False
            }
        }
        mldb.put("/v1/procedures/rocket_science", conf)

        rez = mldb.post("/v1/procedures/rocket_science/runs")
        js_rez = rez.json()
        trained_files = [x["modelFileUrl"].replace("file://", "")
                         for x in js_rez["status"]["folds"]]

        # did we run two training jobs that both got a good auc ?
        self.assertEqual(len(js_rez["status"]["folds"]), 2)
        for i in xrange(2):
            self.assertGreater(
                js_rez["status"]["folds"][i]["resultsTest"]["auc"], 0.95)

        # score using the predictor (MLDB-1070)
        def apply_predictor():
            score_runs = []
            for i in xrange(2):
                rez = mldb.get(
                    "/v1/functions/my_test_exp_scorer_%d/application" % i,
                    input={"features": {"feat1":10 , "feat2": 50}})
                score_runs.append(rez.json()["output"]["score"])
            return score_runs

        score_run1 = apply_predictor()
        self.assertEqual(len(score_run1), 2)

        # did we create two output datasets?
        rez = mldb.get("/v1/datasets")
        js_rez = rez.json()
        self.assertFalse(any(("results_" in x for x in js_rez)))

        ########
        # make sure if we post a slightly modified version of the config and rerun it
        # overrides everything
        trained_files_mod_ts = max([os.path.getmtime(path) for path in trained_files])

        # repost and inverse label
        conf["params"]["inputData"] = \
            "select {* EXCLUDING(label)} as features, NOT label as label from toy"
        mldb.put("/v1/procedures/rocket_science", conf)
        mldb.post("/v1/procedures/rocket_science/runs")

        new_trained_files_mod_ts = \
            min([os.path.getmtime(path) for path in trained_files])

        # make sure the files are newer
        assert trained_files_mod_ts < new_trained_files_mod_ts

        # compare scoring with 1st run (MLDB-1070)
        score_run2 = apply_predictor()
        self.assertNotEqual(set(score_run1), set(score_run2))
        conf["params"]["inputData"] = \
            "select {* EXCLUDING(label)} as features, label from toy"


        #######
        # no split specified
        ######

        del conf["params"]["datasetFolds"]
        conf["params"]["experimentName"] = "no_fold"
        conf["params"]["outputAccuracyDataset"] = True

        mldb.put("/v1/procedures/rocket_science2", conf)

        rez = mldb.post("/v1/procedures/rocket_science2/runs")
        js_rez = rez.json()

        # did we get the rez for 1 fold?
        self.assertEqual(len(js_rez["status"]["folds"]), 1)
        self.assertTrue('accuracyDataset' in js_rez["status"]["folds"][0])

        # did we create the output dataset?
        rez = mldb.get("/v1/datasets")
        js_rez = rez.json()
        self.assertTrue(any(("results_" in x for x in js_rez)))


        #######
        # no split specified
        ######

        del conf["params"]["testingDataOverride"]
        conf["params"]["experimentName"] = "no_fold_&_no_testing"

        rez = mldb.put("/v1/procedures/rocket_science8", conf)
        rez = mldb.post("/v1/procedures/rocket_science8/runs")
        #mldb.log(rez)

        js_rez = rez.json()
        #mldb.log(js_rez)

        # did we run two training jobs that both got a good auc ?
        assert len(js_rez["status"]["folds"]) == 1


        #######
        # 5-fold specified
        ######

        conf["params"]["experimentName"] = "5fold_fold"
        conf["params"]["kfold"] = 5
        conf["params"]["runOnCreation"] = True


        rez = mldb.put("/v1/procedures/rocket_science3", conf)
        js_rez = rez.json()

        # did we run two training jobs that both got a good auc ?
        self.assertEqual(len(js_rez["status"]["firstRun"]["status"]["folds"]),
                         5)

        # make sure all the AUCs are ok
        for fold in js_rez["status"]["firstRun"]["status"]["folds"]:
            self.assertGreater(fold["resultsTest"]["auc"], 0.5,
                               'expect an AUC above 0.5, got ' + str(fold))


        #######
        # 5-fold specified with different datasets
        # should not work
        ######

        conf["params"]["experimentName"] = "5fold_fold_diff_dataset"
        conf["params"]["testingDataOverride"] = \
            "select {* EXCLUDING(label)} as features, label from toy2"
        conf["params"]["kfold"] = 5

        with self.assertRaises(mldb_wrapper.ResponseException) as exc:
            mldb.put("/v1/procedures/rocket_science4", conf)
        mldb.log(exc.exception.response)

        #######
        # default val for different datasets
        ######

        conf["params"]["experimentName"] = "diff_dataset"
        conf["params"]["testingDataOverride"] = \
            "select {* EXCLUDING(label)} as features, label from toy2"
        del conf["params"]["kfold"]

        mldb.put("/v1/procedures/rocket_science5", conf)

        rez = mldb.post("/v1/procedures/rocket_science5/runs")

        js_rez = rez.json()
        self.assertEqual(len(js_rez["status"]["folds"]), 1)
        self.assertTrue(js_rez["status"]["folds"][0]["fold"]["trainingWhere"])
        self.assertTrue(js_rez["status"]["folds"][0]["fold"]["testingWhere"])



        ######
        # missing feature/label for traing or test data
        ######

        #"inputData": "select {* EXCLUDING(label)} as features, label from toy",
        conf["params"]["inputData"] = "select * from toy"
        conf["params"]["testingDataOverride"] = "select * from toy"

        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.put("/v1/procedures/rocket_science5", conf)

        # fix training
        conf["params"]["inputData"] = \
            "select {* EXCLUDING(label)} as features, label from toy"

        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.put("/v1/procedures/rocket_science5", conf)


    def test_eval_training(self):
        conf = {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "my_test_exp",
                "inputData": "select {* EXCLUDING(label)} as features, label from toy",
                "testingDataOverride": "select {* EXCLUDING(label)} as features, label from toy",
                "datasetFolds" : [
                    {
                        "trainingWhere": "rowHash() % 5 != 3",
                        "testingWhere": "rowHash() % 5 = 3",
                    },
                    {
                        "trainingWhere": "rowHash() % 5 != 2",
                        "testingWhere": "rowHash() % 5 = 2",
                    }],
                "modelFileUrlPattern": "file://build/x86_64/tmp/bouya-$runid.cls",
                "algorithm": "glz",
                "equalizationFactor": 0.5,
                "mode": "boolean",
                "configuration": {
                    "glz": {
                        "type": "glz",
                        "verbosity": 3,
                        "normalize": False,
                        "regularization": 'l2'
                    }
                },
                "outputAccuracyDataset": False,
                "evalTrain": True
            }
        }
        rez = mldb.put("/v1/procedures/rocket_science", conf)

        rez = mldb.post("/v1/procedures/rocket_science/runs")
        js_rez = rez.json()

        mldb.log(js_rez)

        # are the training results keys present?
        self.assertTrue("resultsTrain" in js_rez["status"]["folds"][0])

        # performance should be comparable
        self.assertAlmostEqual(
            js_rez["status"]["folds"][0]["resultsTrain"]["auc"],
            js_rez["status"]["folds"][0]["resultsTest"]["auc"],
            delta=0.05)


        # make sure we have the expected results
        self.assertEqual(js_rez["status"]["folds"][0]["functionName"], "my_test_exp_scorer_0")
        mldb.get("/v1/functions/"+js_rez["status"]["folds"][0]["functionName"]) # make sure it exists

        self.assertEqual(js_rez["status"]["folds"][0]["modelFileUrl"], "file://build/x86_64/tmp/bouya-my_test_exp-0.cls")

    def test_no_cls_write_perms(self):
        conf = {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "my_test_no_write",
                "inputData": "select {* EXCLUDING(label)} as features, label from toy",
                "kfold": 2,
                "modelFileUrlPattern": "file:///bouya-$runid.cls",
                "algorithm": "glz",
                "mode": "boolean",
                "configuration": {
                    "glz": {
                        "type": "glz",
                        "verbosity": 3,
                        "normalize": False,
                        "regularization": 'l2'
                    }
                },
                "outputAccuracyDataset": False,
                "runOnCreation": True
            }
        }
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                'Error when trying'):
            mldb.put("/v1/procedures/rocket_science", conf)


    def test_uniqueScoreOutput(self):
        for unique in [True, False]:
            conf = {
                "type": "classifier.experiment",
                "params": {
                    "experimentName": "uniqueScoreOutputTest",
                    "inputData": "select {* EXCLUDING(label)} as features, label from toy",
                    "modelFileUrlPattern": "file://build/x86_64/tmp/bouya-pwet-$runid.cls",
                    "algorithm": "dt",
                    "mode": "boolean",
                    "configuration": {
                        "dt": {
                            "type": "decision_tree",
                            "max_depth": 8,
                            "verbosity": 3,
                            "update_alg": "prob"
                        }
                    },
                    "outputAccuracyDataset": True,
                    "evalTrain": True,
                    "runOnCreation": True,
                    "uniqueScoresOnly": unique
                }
            }
            rez = mldb.put("/v1/procedures/test_uniqueScoreOutput", conf)
            jsRez = rez.json()

            datasetName = jsRez["status"]["firstRun"]["status"]["folds"][0]["accuracyDataset"]

            count = mldb.query("select count(*) from " + datasetName)[1][1]

            # if we're asking for only unique scores
            if unique:
                count2 = mldb.query("""
                    select sum(cnt)
                    from (
                        select count(*) as cnt from %s group by score
                    )""" % datasetName)[1][1]

            # if we're asking for a 1-1 mapping between the output dataset and
            # the test set
            else:
                test_where = jsRez["status"]["firstRun"]["status"]["folds"][0]["fold"]["testingWhere"]
                count2 = mldb.query("select count(*) from toy where %s" % test_where)[1][1]

            self.assertEqual(count, count2)


    def test_limitoffset(self):
        conf = {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "limitoffset",
                "inputData": "select {* EXCLUDING(label)} as features, label from toy",
                "modelFileUrlPattern": "file://build/x86_64/tmp/bouya-pwet-$runid.cls",
                "algorithm": "dt",
                "mode": "boolean",
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 3,
                        "update_alg": "prob"
                    }
                },
                "datasetFolds": [
                    {
                        "trainingLimit": 2500,
                        "testingOffset": 2500
                    },
                    {
                        "testingLimit": 2500,
                        "trainingOffset": 2500
                    }
                ],
                "outputAccuracyDataset": True,
                "evalTrain": False,
                "runOnCreation": True
            }
        }
        rez = mldb.put("/v1/procedures/test_limitoffset", conf)
        jsRez = rez.json()

        datasetNames = [
            jsRez["status"]["firstRun"]["status"]["folds"][0]["accuracyDataset"],
            jsRez["status"]["firstRun"]["status"]["folds"][1]["accuracyDataset"]]

        rowNames = []
        for datasetName in datasetNames:
            rowNames.append(set([x[0] for x in mldb.query("select rowName() from %s" % datasetName)[1:]]))
            self.assertEqual(len(rowNames[-1]), 2500)

        # if the limit and offsets are working, we should get no duplicate rowNames
        # and get back all the rownames in the dataset
        set_union = len(rowNames[0].union(rowNames[1]))
        self.assertEqual(set_union, 5000)

    def test_where_not_accepted(self):
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.post("/v1/procedures", {
                "type": "classifier.experiment",
                "params": {
                    'runOnCreation' : True,
                    "experimentName": "test_no_test_data",
                    "inputData": """
                        SELECT {* EXCLUDING(label)} AS features, label
                        FROM toy WHERE (rowHash() / 128) % 2 = 3""",
                    "modelFileUrlPattern":
                        "file://build/x86_64/tmp/test_no_test_data_$runid.cls",
                    "algorithm": "glz",
                    "equalizationFactor": 0.5,
                    "mode": "boolean",
                    "configuration": {
                        "glz": {
                            "type": "glz",
                            "verbosity": 3,
                            "normalize": False,
                            "regularization": 'l2'
                        }
                    },
                    "outputAccuracyDataset": False
                }
            })

        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.post("/v1/procedures", {
                "type": "classifier.experiment",
                "params": {
                    'runOnCreation' : True,
                    "experimentName": "test_no_test_data",
                    "inputData": """
                        SELECT {* EXCLUDING(label)} AS features, label
                        FROM toy""",
                    "testingDataOverride" : """
                        SELECT {* EXCLUDING(label)} AS features, label
                        FROM toy WHERE (rowHash() / 128) % 2 = 3""",
                    "modelFileUrlPattern":
                        "file://build/x86_64/tmp/test_no_test_data_$runid.cls",
                    "algorithm": "glz",
                    "equalizationFactor": 0.5,
                    "mode": "boolean",
                    "configuration": {
                        "glz": {
                            "type": "glz",
                            "verbosity": 3,
                            "normalize": False,
                            "regularization": 'l2'
                        }
                    },
                    "outputAccuracyDataset": False
                }
            })

    def test_limit_not_accepted(self):
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.post("/v1/procedures", {
                "type": "classifier.experiment",
                "params": {
                    'runOnCreation' : True,
                    "experimentName": "test_no_test_data",
                    "inputData": """
                        SELECT {* EXCLUDING(label)} AS features, label
                        FROM toy LIMIT 1""",
                    "modelFileUrlPattern":
                        "file://build/x86_64/tmp/test_no_test_data_$runid.cls",
                    "algorithm": "glz",
                    "equalizationFactor": 0.5,
                    "mode": "boolean",
                    "configuration": {
                        "glz": {
                            "type": "glz",
                            "verbosity": 3,
                            "normalize": False,
                            "regularization": 'l2'
                        }
                    },
                    "outputAccuracyDataset": False
                }
            })

        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.post("/v1/procedures", {
                "type": "classifier.experiment",
                "params": {
                    'runOnCreation' : True,
                    "experimentName": "test_no_test_data",
                    "inputData": """
                        SELECT {* EXCLUDING(label)} AS features, label
                        FROM toy""",
                    "testingDataOverride" : """
                        SELECT {* EXCLUDING(label)} AS features, label
                        FROM toy LIMIT 1""",
                    "modelFileUrlPattern":
                        "file://build/x86_64/tmp/test_no_test_data_$runid.cls",
                    "algorithm": "glz",
                    "equalizationFactor": 0.5,
                    "mode": "boolean",
                    "configuration": {
                        "glz": {
                            "type": "glz",
                            "verbosity": 3,
                            "normalize": False,
                            "regularization": 'l2'
                        }
                    },
                    "outputAccuracyDataset": False
                }
            })

    def test_orderby(self):
        mldb.put('/v1/datasets/ds_ob', {
            'type': 'sparse.mutable'
        })

        n=200
        for i in xrange(n):
            mldb.post('/v1/datasets/ds_ob/rows', {
                'rowName': str(i),
                'columns': [['order', random(), 0]]
                            + [['x' + str(j), gauss(0,1), 0]
                                for j in xrange(20)]
                            + [['label', random() > 1, 0]]


            })
        mldb.post('/v1/datasets/ds_ob/commit')

        conf = {
            'type': 'classifier.experiment',
            'params': {
                'inputData': 'select {* EXCLUDING (label, order)} as features,'
                             'label from ds_ob',
                'datasetFolds': [
                    # training on the first half and testing on the second
                    {
                        'trainingLimit': n // 2,
                        'trainingOrderBy': 'order',
                        'testingOffset': n // 2,
                        'testingOrderBy': 'order'
                    },
                ],
                'mode': 'boolean',
                'algorithm': 'dt',
                'modelFileUrlPattern': 'file://tmp/test_orderby_$runid',
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 3,
                        "update_alg": "prob"
                    }
                },
                'runOnCreation': True
            }
        }

        # running twice should yield the same result
        res1 = mldb.post('/v1/procedures', conf).json()['status']['firstRun'] \
            ['status']['folds'][0]['resultsTest']['auc']
        res2 = mldb.post('/v1/procedures', conf).json()['status']['firstRun'] \
            ['status']['folds'][0]['resultsTest']['auc']
        self.assertEqual(res1, res2)

    def test_offset_not_accepted(self):
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.post("/v1/procedures", {
                "type": "classifier.experiment",
                "params": {
                    'runOnCreation' : True,
                    "experimentName": "test_no_test_data",
                    "inputData": """
                        SELECT {* EXCLUDING(label)} AS features, label
                        FROM toy OFFSET 1""",
                    "modelFileUrlPattern":
                        "file://build/x86_64/tmp/test_no_test_data_$runid.cls",
                    "algorithm": "glz",
                    "equalizationFactor": 0.5,
                    "mode": "boolean",
                    "configuration": {
                        "glz": {
                            "type": "glz",
                            "verbosity": 3,
                            "normalize": False,
                            "regularization": 'l2'
                        }
                    },
                    "outputAccuracyDataset": False
                }
            })

        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.post("/v1/procedures", {
                "type": "classifier.experiment",
                "params": {
                    'runOnCreation' : True,
                    "experimentName": "test_no_test_data",
                    "inputData": """
                        SELECT {* EXCLUDING(label)} AS features, label
                        FROM toy""",
                    "testingDataOverride" : """
                        SELECT {* EXCLUDING(label)} AS features, label
                        FROM toy OFFSET 1""",
                    "modelFileUrlPattern":
                        "file://build/x86_64/tmp/test_no_test_data_$runid.cls",
                    "algorithm": "glz",
                    "equalizationFactor": 0.5,
                    "mode": "boolean",
                    "configuration": {
                        "glz": {
                            "type": "glz",
                            "verbosity": 3,
                            "normalize": False,
                            "regularization": 'l2'
                        }
                    },
                    "outputAccuracyDataset": False
                }
            })


if __name__ == '__main__':
    mldb.run_tests()
