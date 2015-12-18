# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json, random, datetime, os

## Create toy dataset
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
                "ridge_regression": True
            }
        },
        "outputAccuracyDataset": False
    }
}
rez = mldb.perform("PUT", "/v1/procedures/rocket_science", [], conf)
mldb.log(json.loads(rez["response"]))

rez = mldb.perform("POST", "/v1/procedures/rocket_science/runs")
mldb.log(rez)
assert rez["statusCode"] == 201

jsRez = json.loads(rez["response"])
mldb.log(jsRez)
trained_files = [x["modelFileUrl"].replace("file://", "") for x in jsRez["status"]["folds"]]

# did we run two training jobs that both got a good auc ?
assert len(jsRez["status"]["folds"]) == 2
for i in xrange(2):
    assert jsRez["status"]["folds"][i]["results"]["auc"] > 0.95, 'expected AUC to be above 0.95'

# score using the predictor (MLDB-1070)
def applyPredictor():
    score_runs = []
    for i in xrange(2):
        rez = mldb.perform("GET", "/v1/functions/my_test_exp_scorer_%d/application" % i,
                    [["input",  {"features": {"feat1":10 , "feat2": 50}} ]])
        score_runs.append(json.loads(rez["response"])["output"]["score"])
    return score_runs

score_run1 = applyPredictor()
assert len(score_run1) == 2
mldb.log(score_run1)


# did we create two output datasets?
rez = mldb.perform("GET", "/v1/datasets")
jsRez = json.loads(rez["response"])
mldb.log(jsRez)
assert not any(("results_" in x for x in jsRez))



########
# make sure if we post a slightly modified version of the config and rerun it
# overrides everything
trained_files_mod_ts = max([os.path.getmtime(path) for path in trained_files])
mldb.log(trained_files_mod_ts)

# repost and inverse label
conf["params"]["trainingData"] = "select {* EXCLUDING(label)} as features, NOT label as label from toy"
rez = mldb.perform("PUT", "/v1/procedures/rocket_science", [], conf)
assert rez["statusCode"] == 201, 'procedure creation failed with :' + rez['response']

rez = mldb.perform("POST", "/v1/procedures/rocket_science/runs")
assert rez["statusCode"] == 201, 'procedure run faild with :' + rez['response']

new_trained_files_mod_ts = min([os.path.getmtime(path) for path in trained_files])
mldb.log(new_trained_files_mod_ts)

# make sure the files are newer
assert trained_files_mod_ts < new_trained_files_mod_ts

# compare scoring with 1st run (MLDB-1070)
score_run2 = applyPredictor()
mldb.log(score_run2)
assert set(score_run1) != set(score_run2)
conf["params"]["trainingData"] = "select {* EXCLUDING(label)} as features, label from toy"


#######
##no split specified
######

del conf["params"]["datasetFolds"]
conf["params"]["experimentName"] = "no_fold"
conf["params"]["outputAccuracyDataset"] = True

rez = mldb.perform("PUT", "/v1/procedures/rocket_science2", [], conf)
mldb.log(rez)

rez = mldb.perform("POST", "/v1/procedures/rocket_science2/runs")
mldb.log(rez)
assert rez["statusCode"] == 201

jsRez = json.loads(rez["response"])
mldb.log(jsRez)

# did we run two training jobs that both got a good auc ?
assert len(jsRez["status"]["folds"]) == 1

# did we create two output datasets?
rez = mldb.perform("GET", "/v1/datasets")
jsRez = json.loads(rez["response"])
mldb.log(jsRez)
assert any(("results_" in x for x in jsRez))


#######
##no split specified
######

del conf["params"]["testingData"]
conf["params"]["experimentName"] = "no_fold_&_no_testing"

rez = mldb.perform("PUT", "/v1/procedures/rocket_science8", [], conf)
mldb.log(rez)

rez = mldb.perform("POST", "/v1/procedures/rocket_science8/runs")
mldb.log(rez)
assert rez["statusCode"] == 201, 'procedure run failed with ' + rez['response']

jsRez = json.loads(rez["response"])
mldb.log(jsRez)


# did we run two training jobs that both got a good auc ?
assert len(jsRez["status"]["folds"]) == 1


#######
## 5-fold specified
######

conf["params"]["experimentName"] = "5fold_fold"
conf["params"]["kfold"] =  5
conf["params"]["runOnCreation"] = True


rez = mldb.perform("PUT", "/v1/procedures/rocket_science3", [], conf)
mldb.log(conf)
mldb.log(rez)
assert rez["statusCode"] == 201

jsRez = json.loads(rez["response"])
mldb.log(jsRez)


# did we run two training jobs that both got a good auc ?
assert len(jsRez["status"]["firstRun"]["status"]["folds"]) == 5

# make sure all the AUCs are ok
for fold in jsRez["status"]["firstRun"]["status"]["folds"]:
    assert fold["results"]["auc"] > 0.5, 'expect an AUC above 0.5, got ' + str(fold)


#######
## 5-fold specified with different datasets
## should not work
######

conf["params"]["experimentName"] = "5fold_fold_diff_dataset"
conf["params"]["testingData"] =  "select {* EXCLUDING(label)} as features, label from toy2"
conf["params"]["kfold"] = 5

rez = mldb.perform("PUT", "/v1/procedures/rocket_science4", [], conf)
mldb.log(rez)
assert rez["statusCode"] == 400



#######
## default val for different datasets
######

conf["params"]["experimentName"] = "diff_dataset"
conf["params"]["testingData"] =  "select {* EXCLUDING(label)} as features, label from toy2"
del conf["params"]["kfold"]

rez = mldb.perform("PUT", "/v1/procedures/rocket_science5", [], conf)
mldb.log(rez)

rez = mldb.perform("POST", "/v1/procedures/rocket_science5/runs")
mldb.log(rez)
assert rez["statusCode"] == 201

jsRez = json.loads(rez["response"])
assert len(jsRez["status"]["folds"]) == 1
assert jsRez["status"]["folds"][0]["fold"]["training_where"] == "true"
assert jsRez["status"]["folds"][0]["fold"]["testing_where"] == "true"



mldb.script.set_return("success")

