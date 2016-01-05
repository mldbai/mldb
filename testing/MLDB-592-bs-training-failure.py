# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import csv, datetime, urllib, json

######################
##  Create toy dataset
######################
mldb.perform("DELETE", "/v1/datasets/toy", [], {})

# create a mutable beh dataset
datasetConfig = {
        "type": "sparse.mutable",
        "id": "toy",
    }

dataset = mldb.create_dataset(datasetConfig)

def featProc(k, v):
    if k=="Pclass": return "c"+v
    if k=="Cabin": return v[0]
    return v

ts = datetime.datetime.now()
titanic_dataset = "https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv"
for idx, csvLine in enumerate(csv.DictReader(urllib.urlopen(titanic_dataset))):
    tuples = [[k,featProc(k,v),ts] for k,v in csvLine.iteritems() if k != "PassengerId" and v!=""]
    dataset.record_row(csvLine["PassengerId"], tuples)

# commit the dataset
dataset.commit()


######################
##  Train models!!
######################
def getClsConfig(pipe_id, algo):
    return {
        "id": pipe_id,
        "params": {
            "modelFileUrl": "file://models/mldb-592-test-%s.cls" % pipe_id,
            "configuration": algo,
            "trainingData": "select {* EXCLUDING (label)} as features, label='1' as label from toy where true",
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
    fullAlgo = getClsConfig(algoName, algoConf)

    # Train
    print mldb.perform("PUT", "/v1/procedures/%s" % algoName, [], fullAlgo)
    print mldb.perform("PUT", "/v1/procedures/%s/runs/1" % algoName, [], {})

    # Create function
    functionName = "mldb-592-test-%s-function" % algoName
    applyFunctionConfig = {
        "id": functionName,
        "type": "classifier",
        "params": {
            "modelFileUrl": "file://models/mldb-592-test-%s.cls" % algoName,
        }
    }
    print mldb.perform("PUT", str("/v1/functions/"+functionName), [], applyFunctionConfig)

    # Run eval
    testPipe = "mldb-592-test-%s-eval-procedure" % algoName
    evalConfig = {
        "id": testPipe,
        "type": "classifier.test",
        "params": {
            "testingData": """label='1' as label,
                              APPLY FUNCTION \"%s\" WITH (object(* EXCLUDING (Label)) AS features) EXTRACT(score) as score
                              from toy where true""" % functionName,
            "outputDataset": {
                "id": "mldb-592-test-%s-output" % algoName,
                "type": "sparse.mutable"
            }
        }
    }

    print mldb.perform("PUT",  str("/v1/procedures/%s" % testPipe), [], evalConfig)
    rtn = mldb.perform("POST", str("/v1/procedures/%s/runs" % testPipe), [], {})

    mldb.log(str(type(rtn)))
    mldb.log(json.dumps(rtn))
    mldb.log(" ---- ")
    
    if rtn["statusCode"] == 400:
        broken = True
        mldb.log("ERROR CODE 400!!! for " + algoName)
        break

if not broken:
    mldb.script.set_return("success")
else:
    mldb.script.set_return("FAILURE")


