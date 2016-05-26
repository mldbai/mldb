#
# MLDB-878_experiment_proc.py
# datacratic, 2015
# this file is part of mldb. copyright 2015 datacratic. all rights reserved.
#
import random, datetime, os

import unittest

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb878Test(MldbUnitTest):  

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
                label = random.random() < 0.2
                dataset.record_row("u%d" % i, [["feat1", random.gauss(5 if label else 15, 3), now],
                                               ["feat2", random.gauss(-5 if label else 10, 10), now],
                                               ["label", label, now]])

            dataset.commit()

    def test_all(self):
        conf = {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "my_test_exp",
                "trainingData": "select {* EXCLUDING(label)} as features, label from toy",
                "testingData": "select {* EXCLUDING(label)} as features, label from toy",
                "datasetFolds" : [
                    {
                        "training_where": "rowHash() % 5 != 3",
                        "testing_where": "rowHash() % 5 = 3",
                        "orderBy": "rowHash() ASC",
                    },
                    {
                        "training_where": "rowHash() % 5 != 2",
                        "testing_where": "rowHash() % 5 = 2",
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
                        "link": "linear",
                        "regularization": 'l2'
                    }
                },
                "outputAccuracyDataset": False
            }
        }
        rez = mldb.put("/v1/procedures/rocket_science", conf)
        #mldb.log(rez.json())

        rez = mldb.post("/v1/procedures/rocket_science/runs")
        js_rez = rez.json()
        #mldb.log(js_rez)
        trained_files = [x["modelFileUrl"].replace("file://", "")
                         for x in js_rez["status"]["folds"]]

        # did we run two training jobs that both got a good auc ?
        assert len(js_rez["status"]["folds"]) == 2
        for i in xrange(2):
            assert js_rez["status"]["folds"][i]["resultsTest"]["auc"] > 0.95, \
                'expected AUC to be above 0.95'

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
        assert len(score_run1) == 2
        #mldb.log(score_run1)

        # did we create two output datasets?
        rez = mldb.get("/v1/datasets")
        js_rez = rez.json()
        #mldb.log(js_rez)
        assert not any(("results_" in x for x in js_rez))

        ########
        # make sure if we post a slightly modified version of the config and rerun it
        # overrides everything
        trained_files_mod_ts = max([os.path.getmtime(path) for path in trained_files])
        #mldb.log(trained_files_mod_ts)

        # repost and inverse label
        conf["params"]["trainingData"] = "select {* EXCLUDING(label)} as features, NOT label as label from toy"
        mldb.put("/v1/procedures/rocket_science", conf)
        mldb.post("/v1/procedures/rocket_science/runs")

        new_trained_files_mod_ts = min([os.path.getmtime(path) for path in trained_files])
        #mldb.log(new_trained_files_mod_ts)

        # make sure the files are newer
        assert trained_files_mod_ts < new_trained_files_mod_ts

        # compare scoring with 1st run (MLDB-1070)
        score_run2 = apply_predictor()
        #mldb.log(score_run2)
        assert set(score_run1) != set(score_run2)
        conf["params"]["trainingData"] = "select {* EXCLUDING(label)} as features, label from toy"


        #######
        # no split specified
        ######

        del conf["params"]["datasetFolds"]
        conf["params"]["experimentName"] = "no_fold"
        conf["params"]["outputAccuracyDataset"] = True

        rez = mldb.put("/v1/procedures/rocket_science2", conf)

        rez = mldb.post("/v1/procedures/rocket_science2/runs")
        js_rez = rez.json()

        # did we get the rez for 1 fold?
        assert len(js_rez["status"]["folds"]) == 1
        accuracyDataset = js_rez["status"]["folds"][0]["accuracyDataset"]

        # did we create the output dataset?
        rez = mldb.get("/v1/datasets")
        js_rez = rez.json()
        #mldb.log(js_rez)
        assert any(("results_" in x for x in js_rez))


        #######
        # no split specified
        ######

        del conf["params"]["testingData"]
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
        #mldb.log(conf)
        #mldb.log(rez)

        js_rez = rez.json()
        #mldb.log(js_rez)

        # did we run two training jobs that both got a good auc ?
        assert len(js_rez["status"]["firstRun"]["status"]["folds"]) == 5

        # make sure all the AUCs are ok
        for fold in js_rez["status"]["firstRun"]["status"]["folds"]:
            assert fold["resultsTest"]["auc"] > 0.5, \
                'expect an AUC above 0.5, got ' + str(fold)


        #######
        # 5-fold specified with different datasets
        # should not work
        ######

        conf["params"]["experimentName"] = "5fold_fold_diff_dataset"
        conf["params"]["testingData"] = \
            "select {* EXCLUDING(label)} as features, label from toy2"
        conf["params"]["kfold"] = 5

        try:
            mldb.put("/v1/procedures/rocket_science4", conf)
        except mldb_wrapper.ResponseException as exc:
            mldb.log(exc.response)
        else:
            assert False, 'should not be here'

        #######
        # default val for different datasets
        ######

        conf["params"]["experimentName"] = "diff_dataset"
        conf["params"]["testingData"] = \
            "select {* EXCLUDING(label)} as features, label from toy2"
        del conf["params"]["kfold"]

        rez = mldb.put("/v1/procedures/rocket_science5", conf)
        #mldb.log(rez)

        rez = mldb.post("/v1/procedures/rocket_science5/runs")
        #mldb.log(rez)

        js_rez = rez.json()
        assert len(js_rez["status"]["folds"]) == 1
        assert js_rez["status"]["folds"][0]["fold"]["training_where"] == "true"
        assert js_rez["status"]["folds"][0]["fold"]["testing_where"] == "true"



        ######
        # missing feature/label for traing or test data
        ######

        #"trainingData": "select {* EXCLUDING(label)} as features, label from toy",
        conf["params"]["trainingData"] = "select * from toy"
        conf["params"]["testingData"] = "select * from toy"

        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/procedures/rocket_science5", conf)

        # fix training
        conf["params"]["trainingData"] = "select {* EXCLUDING(label)} as features, label from toy"

        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/procedures/rocket_science5", conf)
    

    def test_eval_training(self):
        conf = {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "my_test_exp",
                "trainingData": "select {* EXCLUDING(label)} as features, label from toy",
                "testingData": "select {* EXCLUDING(label)} as features, label from toy",
                "datasetFolds" : [
                    {
                        "training_where": "rowHash() % 5 != 3",
                        "testing_where": "rowHash() % 5 = 3",
                        "orderBy": "rowHash() ASC",
                    },
                    {
                        "training_where": "rowHash() % 5 != 2",
                        "testing_where": "rowHash() % 5 = 2",
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
                        "link": "linear",
                        "regularization": 'l2'
                    }
                },
                "outputAccuracyDataset": False,
                "evalTrain": True
            }
        }
        rez = mldb.put("/v1/procedures/rocket_science", conf)
        #mldb.log(rez.json())

        rez = mldb.post("/v1/procedures/rocket_science/runs")
        js_rez = rez.json()

        mldb.log(js_rez)

        # are the training results keys present?
        self.assertTrue("resultsTrain" in js_rez["status"]["folds"][0])

        # performance should be comparable
        self.assertAlmostEqual(js_rez["status"]["folds"][0]["resultsTrain"]["auc"],
                               js_rez["status"]["folds"][0]["resultsTest"]["auc"], delta=0.05)
    

    def test_no_cls_write_perms(self):
        conf = {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "my_test_no_write",
                "trainingData": "select {* EXCLUDING(label)} as features, label from toy",
                "kfold": 2,
                "modelFileUrlPattern": "file:///bouya-$runid.cls",
                "algorithm": "glz",
                "mode": "boolean",
                "configuration": {
                    "glz": {
                        "type": "glz",
                        "verbosity": 3,
                        "normalize": False,
                        "link": "linear",
                        "regularization": 'l2'
                    }
                },
                "outputAccuracyDataset": False,
                "runOnCreation": True
            }
        }
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                'Error when trying'):
            rez = mldb.put("/v1/procedures/rocket_science", conf)


    def test_uniqueScoreOutput(self):
        counts = {}
        for unique in [True, False]:
            conf = {
                "type": "classifier.experiment",
                "params": {
                    "experimentName": "uniqueScoreOutputTest",
                    "trainingData": "select {* EXCLUDING(label)} as features, label from toy",
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
                test_where = jsRez["status"]["firstRun"]["status"]["folds"][0]["fold"]["testing_where"]
                #mldb.log("select count(*) from toy where %s" % test_where)
                count2 = mldb.query("select count(*) from toy where %s" % test_where)[1][1]

            self.assertEqual(count, count2)



mldb.run_tests()

