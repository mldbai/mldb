#
# MLDB-592-bs-training-failure.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
import csv, datetime, urllib

mldb = mldb_wrapper.wrap(mldb) # noqa

######################
##  Create toy dataset
######################
mldb.delete("/v1/datasets/toy")

# create a mutable beh dataset
datasetConfig = {
    "type": "sparse.mutable",
    "id": "toy",
}

dataset = mldb.create_dataset(datasetConfig)

def feat_proc(k, v):
    if k == "Pclass": return "c"+v
    if k == "Cabin": return v[0]
    return v

ts = datetime.datetime.now()
titanic_dataset = \
    "https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv"
for idx, csvLine in enumerate(csv.DictReader(urllib.urlopen(titanic_dataset))):
    tuples = [[k,feat_proc(k,v),ts] for k,v in csvLine.iteritems()
              if k != "PassengerId" and v!=""]
    dataset.record_row(csvLine["PassengerId"], tuples)

# commit the dataset
dataset.commit()


######################
##  Train models!!
######################
def get_cls_config(pipe_id, algo):
    return {
        "id": pipe_id,
        "params": {
            "modelFileUrl": "file://models/mldb-592-test-%s.cls" % pipe_id,
            "configuration": algo,
            "trainingData":
                "select {* EXCLUDING (label)} as features, label='1' as label from toy where true",
            "equalizationFactor": 0,
            "mode": "boolean"
        },
        "type": "classifier.train"
    }


dtAlgo = {
    "max_depth": 8,
    "type": "decision_tree",
    "update_alg": "prob",
    "verbosity": 3
}
bsAlgo = {
    "max_iter": 200,
    "min_iter": 10,
    "type": "boosted_stumps",
    "update_alg": "gentle",
    "verbosity": 3
}

broken = False
for algoName, algoConf in [["dtAlgo", dtAlgo], ["bsAlgo", bsAlgo]]:
    mldb.log(">>>>>> Running for algo: " + algoName)
    fullAlgo = get_cls_config(algoName, algoConf)

    # Train
    print mldb.put("/v1/procedures/%s" % algoName, fullAlgo)
    print mldb.put("/v1/procedures/%s/runs/1" % algoName, {})

    # Create function
    functionName = "mldb-592-test-%s-function" % algoName
    applyFunctionConfig = {
        "id": functionName,
        "type": "classifier",
        "params": {
            "modelFileUrl": "file://models/mldb-592-test-%s.cls" % algoName,
        }
    }
    print mldb.put(str("/v1/functions/" + functionName), applyFunctionConfig)

    # Run eval
    testPipe = "mldb-592-test-%s-eval-procedure" % algoName
    evalConfig = {
        "id": testPipe,
        "type": "classifier.test",
        "params": {
            "testingData": """select label='1' as label,
                              \"%s\"({{* EXCLUDING (Label)} AS features})[score] as score
                              from toy""" % functionName,
            "outputDataset": {
                "id": "mldb-592-test-%s-output" % algoName,
                "type": "sparse.mutable"
            }
        }
    }

    print mldb.put(str("/v1/procedures/%s" % testPipe), evalConfig)
    rtn = mldb.post(str("/v1/procedures/%s/runs" % testPipe))

    mldb.log(str(type(rtn)))
    mldb.log(rtn.text)
    mldb.log(" ---- ")

mldb.script.set_return("success")
