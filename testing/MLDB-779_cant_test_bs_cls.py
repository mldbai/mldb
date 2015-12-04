# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


#################
#### This test is for issues MLDB-779 AND MLDB-780
####    We do the training pipelines twice and each cls has a different
####    failure point
#################

import datetime, json, random

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy'
}

dataset = mldb.create_dataset(dataset_config)
mldb.log("data loader created dataset")

now = datetime.datetime.now()

for i in xrange(200):
    label = i % 3 == 0
    feats = []
    for x in xrange(25):
        rnd = random.random()
        if rnd < x/25. or (label is True and rnd < 0.4):
            feats.append(["feat%d" % x, 1, now])
        #else:
        #    feats.append(["feat%d" % x, 0, now])
            

    feats.append(["LABEL", "true" if label else "false", now])
    dataset.record_row("example-%d" % i, feats)

mldb.log("Committing dataset")
dataset.commit()

for cls in ["bdt", "glz", "bs"]:
    ############
    ### train a cls
    mldb.perform("DELETE", "/v1/procedures/tng_classif")
    rez = mldb.perform("PUT", "/v1/procedures/tng_classif", [], {
            "type": "classifier.train",
            "params": {
                "trainingDataset": { "id": "toy" },
                "configuration": {
                    "glz": {
                        "type": "glz",
                        "verbosity": 3,
                        "normalize": False,
                        "link_function": 'linear',
                        "ridge_regression": False
                    },
                    "bs": {
                        "type": "boosted_stumps",
                        "min_iter": 10,
                        "max_iter": 200,
                        "update_alg": "gentle",
                        "verbosity": 3
                    },
                    "bdt": {
                        "type": "boosting",
                        "min_iter": 10,
                        "max_iter": 200,
                        "weak_learner": {
                            "type": "decision_tree",
                            "max_depth": 1
                         }
                    }
                },
                "algorithm": cls,
                "modelFileUrl": "file://models/tng.cls",
                "label": "LABEL = 'true'",
                "weight": "1.0",
                "where": "rowHash() % 3 != 1",
                "select": "* EXCLUDING(LABEL)"
            }
        })

    mldb.log(rez)

    rez = mldb.perform("PUT", "/v1/procedures/tng_classif/runs/1")
    mldb.log(rez)
    # this is where the bs fails
    assert rez["statusCode"] < 400

    rez = mldb.perform("GET", "/v1/procedures/tng_classif/runs/1/details");
    mldb.log(json.loads(rez["response"]))


    ##########
    ## now test it
    mldb.perform("DELETE", "/v1/functions/tng_scorer")
    rez = mldb.perform("PUT", "/v1/functions/tng_scorer", [], {
        "type": "classifier",
        "params": { "modelFileUrl": "file://models/tng.cls" }
    })
    mldb.log(rez)
    assert rez["statusCode"] < 400

    mldb.perform("DELETE", "/v1/procedures/tng_score_proc")
    mldb.perform("DELETE", "/v1/datasets/toy_cls_baseline_scorer_rez")

    rez = mldb.perform("PUT", "/v1/procedures/tng_score_proc", [], {
        "type": "classifier.test",
        "params": {
            "testingDataset": { "id": "toy" },
            "outputDataset": { "id":"toy_cls_baseline_scorer_rez", "type": "sparse.mutable" },
            "where": "rowHash() % 3 = 1",
            "label": "LABEL = 'true'",
            "weight": "1.0",
            "score": "tng_scorer({{* EXCLUDING(LABEL)} as features})[score]",
        }
    })
    mldb.log(rez)
    assert rez["statusCode"] < 400

    rez = mldb.perform("POST", "/v1/procedures/tng_score_proc/runs")
    mldb.log(rez)
    assert rez["statusCode"] < 400


    mldb.script.set_return("success")


    ######
    # create explain function
    mldb.perform("DELETE", "/v1/functions/tng_explain")
    rez = mldb.perform("PUT", "/v1/functions/tng_explain", [], {
        "type": "classifier.explain",
        "params": {
            "modelFileUrl": "file://models/tng.cls"
        }
    })
    mldb.log(json.loads(rez["response"]))
    assert rez["statusCode"] < 400

    # this currently fails with the glz with a "Feature_Set::operator []: feature not found\
    rez = mldb.perform("GET", "/v1/query", [["q", "select tng_explain({{* EXCLUDING(LABEL)} as features, 1 as label})[explanation], * from toy where rowHash() % 3 = 1"], ["format", "sparse"]])

    mldb.log(json.loads(rez["response"]))
    assert rez["statusCode"] < 400

