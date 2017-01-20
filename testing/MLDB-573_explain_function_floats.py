#
# MLDB-573_explain_function_floats.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
import random

mldb = mldb_wrapper.wrap(mldb) # noqa

dataset = mldb.create_dataset({
        "type": "sparse.mutable",
        "id": "x"
    })

surfaces = ["grass", "clay", "hard", "carpet"]
for r in xrange(1000):
    val = float(random.random())
    tuples = []
    tuples.append(["ProbWin", val, 0])
    tuples.append(["Rank", random.randint(1,1000), 0])
    if val < 0.1 :
        tuples.append(["label", 1, 0])
    else:
        tuples.append(["label", 0, 0])
    tuples.append(["Year", random.randint(2000, 2014), 0])
    surface = surfaces[random.randint(0,3)]
    tuples.append(["Surface", surface,  0])
    dataset.record_row("game_" + str(r), tuples)
dataset.commit()

train_classifier_procedure_config = {
    "id":"float_encoding_cls_train",
    "type":"classifier.train",
    "params":{
        "trainingData":{
            "where":"Year < 2014 AND rowHash() != 1",
            "select":"{* EXCLUDING(label)} as features,  label = 1 as label",
            "from" : {"id":"x"}
        },
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
        "modelFileUrl":"file://tmp/MLDB-573-float_encoding.cls"
        }
    }
procedure_output = mldb.put("/v1/procedures/float_encoding_cls_train",
                            train_classifier_procedure_config)
mldb.log("the procedure output is " + procedure_output.text)

training_output = mldb.put("/v1/procedures/float_encoding_cls_train/runs/1")
mldb.log("training output:" + training_output.text)

function_config = {
    "id" : "classifyFunction",
    "type": "classifier",
    "params": {"modelFileUrl":"file://tmp/MLDB-573-float_encoding.cls"}
}
function_output = mldb.put("/v1/functions/classifyFunction", function_config)
mldb.log("the function output " + function_output.text)

# now train a probabilizer

train_probabilizer_procedure_config = {
    "id":"float_encoding_prob_train",
    "type":"probabilizer.train",
    "params":{
        "trainingData": {
            "select":
                "classifyFunction({{* EXCLUDING (label)} as features})[score] as score, label = 1 as label",
             "from": {"id": "x"},
            "where": "Year < 2014 AND rowHash() % 5 = 1"
        },
        "modelFileUrl": "file://tmp/MLDB-573-probabilizer.json",
    }
}

prob_procedure_output = mldb.put("/v1/procedures/float_encoding_prob_train",
                                 train_probabilizer_procedure_config)
mldb.log("The procedure probabilizer output : " + prob_procedure_output.text)

train_prob_result = mldb.put("/v1/procedures/float_encoding_prob_train/runs/1")
mldb.log("The train probabilizer runs : " + prob_procedure_output.text)


#add an explain function
expl_function_config = {
    "id":"explainFunction",
    "type":"classifier.explain",
    "params": {
        "modelFileUrl":"file://tmp/MLDB-573-float_encoding.cls"
        }
    }
explain_function_output = mldb.put(
    "/v1/functions/" + expl_function_config["id"], expl_function_config)
mldb.log("THe resulf of the explain function creation "
         + explain_function_output.text)

rest_params = {
    "input": {
        "features": {"Surface": "clay", "ProbWin": 0.2, "Rank": 10}
    }
}


rest_params["input"]["label"] = 1
# now call the expain function
expl_res = mldb.get("/v1/functions/explainFunction/application",
                    **rest_params)
mldb.log("Result of explain function : " + expl_res.text)

mldb.script.set_return("success")
