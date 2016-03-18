#
# MLDB-174-regression.py
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#

import unittest, json
mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb174Test(MldbUnitTest):  

    @classmethod
    def setUpClass(self):
        # create a dataset for very simple linear regression, with x = y
        ds = mldb.create_dataset({ "id": "test", "type": "sparse.mutable" })
        for row, x, y in ( ("ex1", 0, 0), ("ex2", 1, 1), ("ex3", 2, 2), ("ex4", 3, 3)):
            ds.record_row(row, [ [ "x", x, 0 ], ["y", y, 0] ])
        ds.commit()

        self.cls_conf =  {
            "glz": {
                "type": "glz",
                "verbosity": 3,
                "normalize": True,
                "link_function": 'linear',
                "ridge_regression": True
            },
            "dt": {
                "type": "decision_tree",
                "max_depth": 8,
                "verbosity": 3,
                "update_alg": "prob"
            }
        }
        
        rez = mldb.put('/v1/procedures/wine_red', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "https://s3.amazonaws.com/public.mldb.ai/datasets/wine_quality/winequality-red.csv",
                "delimiter": ";",
                "runOnCreation": True,
                "outputDataset": {
                    "id": "wine_red"
                }
            }
        })
        
        rez = mldb.put('/v1/procedures/wine_white', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "https://s3.amazonaws.com/public.mldb.ai/datasets/wine_quality/winequality-white.csv",
                "delimiter": ";",
                "runOnCreation": True,
                "outputDataset": {
                    "id": "wine_white"
                }
            }
        })

        ## create a merged dataset and train on that
        # the problem here is that there are duplicated rowNames
        mldb.put('/v1/procedures/column_adder', {
            "type": "transform",
            "params": {
                "inputData": """
                    SELECT * FROM merge(
                        (
                            SELECT *, 'red' as color
                            FROM wine_red
                        ),
                        (
                            SELECT *, 'white' as color
                            FROM wine_white
                        )
                    )
                """,
                "outputDataset": "wine_full_collision",
                "runOnCreation": True
            }
        })

        # let's recreate by avoiding collissions in and therefore duplicated columns
        mldb.put('/v1/procedures/column_adder', {
            "type": "transform",
            "params": {
                "inputData": """
                    SELECT * FROM merge(
                        (
                            SELECT *, 'red' as color
                            NAMED rowName() + '_red'
                            FROM wine_red
                        ),
                        (
                            SELECT *, 'white' as color
                            NAMED rowName() + '_white'
                            FROM wine_white
                        )
                    )
                """,
                "outputDataset": "wine_full",
                "runOnCreation": True
            }
        })


    def test_select_simple_regression(self):
        modelFileUrl = "file://tmp/MLDB-174.cls"

        trainClassifierProcedureConfig = {
            "type": "classifier.train",
            "params": {
                "trainingData": { 
                    "select": "{x} as features, y as label",
                    "from": { "id": "test" }
                },
                "configuration": self.cls_conf,
                "algorithm": "glz",
                "modelFileUrl": modelFileUrl,
                "mode": "regression",
                "runOnCreation": True
            }
        }

        procedureOutput = mldb.put("/v1/procedures/cls_train", trainClassifierProcedureConfig)
        mldb.log(procedureOutput.json())


        functionConfig = {
            "type": "classifier",
            "params": {
                "modelFileUrl": modelFileUrl
            }
        }

        createFunctionOutput = mldb.put("/v1/functions/regressor", functionConfig)
        mldb.log(createFunctionOutput.json())

        value = 10
        selected = mldb.get("/v1/functions/regressor/application", input = { "features": { "x":value }})

        jsSelected = selected.json()
        mldb.log(jsSelected)
        result = jsSelected["output"]["score"]

        self.assertAlmostEqual(result, value, delta=0.0001)

    def test_wine_quality_merged_regression_glz(self):
        def getConfig(dataset):
            return {
                "type": "classifier.experiment",
                "params": {
                    "trainingData": """
                        select
                        {* EXCLUDING(quality)} as features,
                        quality as label
                        from %s
                    """ % dataset,
                    "datasetFolds": [
                            {
                                "training_where": "rowHash() % 2 = 0", 
                                "testing_where": "rowHash() % 2 = 1"
                            }
                        ],
                    "experimentName": "winer",
                    "modelFileUrlPattern": "file://tmp/MLDB-174-wine.cls",
                    "algorithm": "glz",
                    "configuration": self.cls_conf,
                    "mode": "regression",
                    "runOnCreation": True
                }
            }

        # this should fail because we check for dupes
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put('/v1/procedures/wine_trainer', getConfig("wine_full_collision"))


        # the training should now work
        config = getConfig("wine_full")
        rez = mldb.put('/v1/procedures/wine_trainer', config)

        # check the performance is in the expected range
        self.assertAlmostEqual(rez.json()["status"]["firstRun"]["status"]["folds"][0]["results"]["r2"], 0.28, places=2)

        mldb.log(rez)

        # make sure the trained model used all features
        scorerDetails = mldb.get("/v1/functions/winer_scorer_0/details").json()
        mldb.log(scorerDetails)
        usedFeatures = [x["feature"] for x in scorerDetails["model"]["params"]["features"]]
        mldb.log(usedFeatures)
        self.assertGreater(len(usedFeatures), 2)


        # run an explain over the 
        mldb.put("/v1/functions/explainer", {
            "type": "classifier.explain",
            "params": {
                "modelFileUrl": config["params"]["modelFileUrlPattern"]
            }
        })

        explain_rez = mldb.query("""
                select explainer({{* EXCLUDING(quality)} as features,
                                  quality as label})
                from wine_full
                where rowHash() % 2 = 1
                limit 2
        """)
        
        self.assertEqual(len(explain_rez), 3)

        # TODO an actual test

    def test_wine_quality_merged_regression_dt(self):
        def getConfig(dataset):
            return {
                "type": "classifier.experiment",
                "params": {
                    "trainingData": """
                        select
                        {* EXCLUDING(quality)} as features,
                        quality as label
                        from %s
                    """ % dataset,
                    "datasetFolds": [
                            {
                                "training_where": "rowHash() % 2 = 0", 
                                "testing_where": "rowHash() % 2 = 1"
                            }
                        ],
                    "experimentName": "winer_dt",
                    "modelFileUrlPattern": "file://tmp/MLDB-174-wine_dt.cls",
                    "algorithm": "dt",
                    "configuration": self.cls_conf,
                    "mode": "regression",
                    "runOnCreation": True
                }
            }


        # the training should now work
        config = getConfig("wine_full")
        rez = mldb.put('/v1/procedures/wine_trainer_dt', config)

        # check the performance is in the expected range
        self.assertAlmostEqual(rez.json()["status"]["firstRun"]["status"]["folds"][0]["results"]["r2"], 0.28, delta=0.1)

        mldb.log(rez)


        # run an explain over the 
        mldb.put("/v1/functions/explainer_dt", {
            "type": "classifier.explain",
            "params": {
                "modelFileUrl": config["params"]["modelFileUrlPattern"]
            }
        })

        explain_rez = mldb.query("""
                select explainer_dt({{* EXCLUDING(quality)} as features,
                                  quality as label})
                from wine_full
                where rowHash() % 2 = 1
                limit 2
        """)

        mldb.log(explain_rez)


mldb.run_tests()

