#
# filename
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
# This test is for issues MLDB-779 AND MLDB-780
# We do the training pipelines twice and each cls has a different
# failure point
#
mldb = mldb_wrapper.wrap(mldb) # noqa

import datetime, random

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
    mldb.delete("/v1/procedures/tng_classif")
    rez = mldb.put("/v1/procedures/tng_classif", {
            "type": "classifier.train",
            "params": {
                "trainingData": {
                    "where": "rowHash() % 3 != 1",
                    "select":
                        "{* EXCLUDING(LABEL)} as features, LABEL = 'true' as label",
                    "from" : { "id": "toy" }
                },
                "configuration": {
                    "glz": {
                        "type": "glz",
                        "verbosity": 3,
                        "normalize": False,
                        "link_function": 'linear',
                        "regularization": 'none'
                    },
                    "bs": {
                        "type": "boosted_stumps",
                        "min_iter": 10,
                        "max_iter": 200,
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
                "modelFileUrl": "file://models/tng.cls"
            }
        })

    mldb.log(rez.json())

    rez = mldb.put("/v1/procedures/tng_classif/runs/1")
    mldb.log(rez.json())
    # this is where the bs fails

    rez = mldb.get("/v1/procedures/tng_classif/runs/1/details")
    mldb.log(rez.json())


    ##########
    ## now test it
    mldb.delete("/v1/functions/tng_scorer")
    rez = mldb.put("/v1/functions/tng_scorer", {
        "type": "classifier",
        "params": {"modelFileUrl": "file://models/tng.cls"}
    })
    mldb.log(rez.json)

    mldb.delete("/v1/procedures/tng_score_proc")
    mldb.delete("/v1/datasets/toy_cls_baseline_scorer_rez")

    rez = mldb.put("/v1/procedures/tng_score_proc", {
        "type": "classifier.test",
        "params": {
            "testingData": {
                "select" :
                    "{*} as features, LABEL = 'true' as label, tng_scorer({{* EXCLUDING(LABEL)} as features})[score] as score",
                "from": {"id": "toy" },
                "where" : "rowHash() % 3 = 1"
            },
            "outputDataset": {
                "id":"toy_cls_baseline_scorer_rez",
                "type": "sparse.mutable"
            }
        }
    })
    mldb.log(rez.json())

    rez = mldb.post("/v1/procedures/tng_score_proc/runs")
    mldb.log(rez.json())


    ######
    # create explain function
    mldb.delete("/v1/functions/tng_explain")
    rez = mldb.put("/v1/functions/tng_explain", {
        "type": "classifier.explain",
        "params": {
            "modelFileUrl": "file://models/tng.cls"
        }
    })
    mldb.log(rez.json())

    # this currently fails with the glz with a "Feature_Set::operator []:
    # feature not found\
    rez = mldb.get(
        "/v1/query",
        q="select tng_explain({{* EXCLUDING(LABEL)} as features, 1 as label})[explanation], "
          "* from toy where rowHash() % 3 = 1",
        format="sparse")

    mldb.log(rez.json())

mldb.script.set_return('success')
