# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import random
import json

dataset = mldb.create_dataset({
        "type": "sparse.mutable",
        "id": "x"
    })

surfaces = ["grass", "clay", "hard", "carpet"]
for r in range(1000):
    val = float(random.random())
    tuples = []
    tuples.append(["ProbWin", val, 0])
    tuples.append(["Rank", random.randint(1,1000), 0])
    if val < 0.1 :
        tuples.append(["label", 1, 0])
    else:
        tuples.append(["label", 0, 0])
    tuples.append(["Year", random.randint(2000, 2014), 0])
    surface = surfaces[ random.randint(0,3)]
#    mldb.log("val = " + str(val) + " surface = " + surface)    
    tuples.append(["Surface", surface,  0])
    dataset.record_row("game_" + str(r), tuples)
dataset.commit()

train_classifier_procedure_config = {
    "id":"float_encoding_cls_train",
    "type":"classifier.train",
    "params":{
        "trainingDataset":{"id":"x"},
        "configuration": {        
            "type": "bagging",
            "verbosity": 3,
            "weak_learner": {
                "type": "boosting",
                "verbosity": 3,
                "weak_learner": {
                    "type": "decision_tree",
                    "max_depth": 3,
                    "verbosity": 0,
                    "update_alg": "gentle",
                    "random_feature_propn": 0.5
                },
                "min_iter": 5,
                "max_iter": 30
            },
            "num_bags": 5
        },
        "modelFileUrl":"file://tmp/MLDB-573-float_encoding.cls",
        "where":"Year < 2014 AND rowHash() != 1",
        "select":"* EXCLUDING(label)",
        "label":"label = 1",
        "weight":"1.0"
        }
    }
procedure_output = mldb.perform("PUT","/v1/procedures/float_encoding_cls_train", [], 
                               train_classifier_procedure_config)

assert procedure_output["statusCode"] < 400

mldb.log("the procedure output is " + json.dumps(procedure_output))

training_output = mldb.perform("PUT","/v1/procedures/float_encoding_cls_train/runs/1", [], 
                               {})
mldb.log("training output:" + json.dumps(training_output))

assert training_output["statusCode"] < 400

function_config = {
    "id" : "classifyFunction",
    "type":"classifier",
    "params":{"modelFileUrl":"file://tmp/MLDB-573-float_encoding.cls"}
    }
function_output = mldb.perform("PUT","/v1/functions/classifyFunction", [], function_config)
mldb.log("the function output " + json.dumps(function_output))
assert function_output["statusCode"] < 400

# now train a probabilizer

train_probabilizer_procedure_config = {
    "id":"float_encoding_prob_train",
    "type":"probabilizer.train",
    "params":{
        "trainingDataset":{"id":"x"},
        "modelFileUrl":"file://tmp/MLDB-573-probabilizer.json",
        "where":"Year < 2014 AND rowHash() % 5 = 1",
        "select":"classifyFunction({{* EXCLUDING (label)} as features})[score]",
        "label":"label = 1"
        }
    }

prob_procedure_output = mldb.perform("PUT", "/v1/procedures/float_encoding_prob_train" , 
                   [], 
                   train_probabilizer_procedure_config)
mldb.log("The procedure probabilizer output : " + json.dumps(prob_procedure_output))
assert prob_procedure_output["statusCode"] < 400

train_prob_result = mldb.perform("PUT", "/v1/procedures/float_encoding_prob_train/runs/1",
                                 [], 
                                 {})
mldb.log("The train probabilizer runs : " + json.dumps(train_prob_result))
assert train_prob_result["statusCode"] < 400



probabilizer_function_config = {
    "id":"probabilizer",
    "type":"serial",
    "params":{
        "steps":[
            {
                "id":"classifyFunction"
                },
            {
                "id":"apply_probabilizer",
                "type":"probabilizer",
                "params": {
                    "modelFileUrl":"file://tmp/MLDB-573-probabilizer.json"
                    }
                }
            ]
        }
    }

probabilizer_function_output = mldb.perform("PUT", "/v1/functions/" +probabilizer_function_config["id"],
                                         [], 
                                         probabilizer_function_config)
mldb.log("The result of the probabilizer function config" +json.dumps(probabilizer_function_output))
assert probabilizer_function_output["statusCode"] < 400

#add an explain function
expl_function_config = {
    "id":"explainFunction",
    "type":"classifier.explain",
    "params": {
        "modelFileUrl":"file://tmp/MLDB-573-float_encoding.cls"
        }
    }
explain_function_output = mldb.perform("PUT", "/v1/functions/" + expl_function_config["id"], [], 
                                    expl_function_config)
mldb.log("THe resulf of the explain function creation " + json.dumps(explain_function_output))
assert explain_function_output["statusCode"] < 400


# now call the probabilizer
rest_params = [["input", { "features": {"Surface": "clay", "ProbWin": 0.2, "Rank": 10}}]]

res = mldb.perform("GET", "/v1/functions/probabilizer/application", rest_params)
mldb.log("the result of calling the classifier " + json.dumps(res))

rest_params[0][1]["label"] = 1
#now call the expain function
expl_res = mldb.perform("GET", "/v1/functions/explainFunction/application", rest_params)
mldb.log("Result of explain function : " + json.dumps(expl_res))

if res["statusCode"] == 200 and expl_res["statusCode"] == 200:
    mldb.script.set_return("success")
else:
    mldb.script.set_return("FAILURE")
