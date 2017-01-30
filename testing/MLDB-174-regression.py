#
# MLDB-174-regression.py
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
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

        # create a dataset for very simple linear regression, with x = y
        ds = mldb.create_dataset({ "id": "test2", "type": "sparse.mutable" })
        for row, x, y, label in ( ("ex1", 0, 10, 1), ("ex2", 1, 0, 2), ("ex3", 10, 10, 5), ("ex4", 0, 8, 3)):
            ds.record_row(row, [ [ "x", x, 0 ], ["y", y, 0], ["label", label, 0] ])
        ds.commit()


        ## cls configurations
        self.cls_conf =  {
            "glz": {
                "type": "glz",
                "verbosity": 3,
                "normalize": True,
                "link_function": 'linear',
                "regularization": 'l2'
            },
            "glz_l1": {
                "type": "glz",
                "verbosity": 3,
                "normalize": True,
                "link_function": 'linear',
                "regularization": 'l1',
                "regularization_factor": 0.001
            },
            "dt": {
                "type": "decision_tree",
                "max_depth": 8,
                "verbosity": 3,
                "update_alg": "prob"
            }
        }


        ## load wine datasets
        rez = mldb.put('/v1/procedures/wine_red', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "https://public.mldb.ai/datasets/wine_quality/winequality-red.csv",
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
                "dataFileUrl": "https://public.mldb.ai/datasets/wine_quality/winequality-white.csv",
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

    def test_select_simple_regression_l1(self):

        mldb.log("L1 regression")

        modelFileUrl = "file://tmp/MLDB-174.cls"

        trainClassifierProcedureConfig = {
            "type": "classifier.train",
            "params": {
                "trainingData": {
                    "select": "{x} as features, y as label",
                    "from": { "id": "test" }
                },
                "configuration": self.cls_conf,
                "algorithm": "glz_l1",
                "modelFileUrl": modelFileUrl,
                "mode": "regression",
                "runOnCreation": True
            }
        }

        procedureOutput = mldb.put("/v1/procedures/cls_train_l1", trainClassifierProcedureConfig)
        mldb.log(procedureOutput.json())


        functionConfig = {
            "type": "classifier",
            "params": {
                "modelFileUrl": modelFileUrl
            }
        }

        createFunctionOutput = mldb.put("/v1/functions/regressor_l1", functionConfig)
        mldb.log(createFunctionOutput.json())

        value = 10
        selected = mldb.get("/v1/functions/regressor_l1/application", input = { "features": { "x":value }})

        jsSelected = selected.json()
        mldb.log(jsSelected)
        result = jsSelected["output"]["score"]

        self.assertAlmostEqual(result, value, delta=0.01)

    def test_wine_quality_merged_regression_glz(self):
        def getConfig(dataset):
            return {
                "type": "classifier.experiment",
                "params": {
                    "inputData": """
                        select
                        {* EXCLUDING(quality)} as features,
                        quality as label
                        from %s
                    """ % dataset,
                    "datasetFolds": [
                            {
                                "trainingWhere": "rowHash() % 2 = 0",
                                "testingWhere": "rowHash() % 2 = 1"
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
        self.assertAlmostEqual(rez.json()["status"]["firstRun"]["status"]["folds"][0]["resultsTest"]["r2"], 0.28, places=2)

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

    def test_wine_quality_merged_regression_glz_l1(self):
        """ We are comparing the results to scikit learn's results for the same
            data. I used scikit-learn in the same way we do it with our GLZ.
        """
        # Here is the python code for the same thing in scikit learn
        #
        # import sklearn, csv
        # from numpy import *
        # from sklearn import linear_model
        # assert sklearn.__version__ == '0.17.1'
        #
        # def load_csv(filename):
        #     data = []
        #     for x in csv.DictReader(open(filename), delimiter=';'):
        #         data.append(x)
        #     return data
        #
        # data = load_csv('/Users/simon/Downloads/winequality-red.csv')
        # put them in the same order MLDB puts them
        # keys = ['total sulfur dioxide', 'pH', 'free sulfur dioxide', 'chlorides',
        #         'fixed acidity', 'density', 'volatile acidity', 'residual sugar',
        #         'alcohol', 'citric acid', 'sulphates']
        #
        # X = empty((len(data), len(keys)), dtype='float64')
        # y = empty(len(data), dtype='float64')
        # for i, d in enumerate(data):
        #     for j,k in enumerate(keys):
        #         X[i,j] = float(d[k])
        #     y[i] = float(d['quality'])
        #
        # train_X = X[::2]
        # test_X = X[1::2]
        # train_y = y[::2]
        # test_y = y[1::2]
        #
        # mean_train = train_X.mean(axis=0)
        # mean_train_y = train_y.mean()
        # std_train = train_X.std(axis=0)
        #
        # train_X -= mean_train
        # train_X /= std_train
        # train_X = hstack((train_X, ones(train_X.shape[0]).reshape(-1,1)))
        #
        # # train_y -= mean_train_y
        #
        # test_X -= mean_train
        # test_X /= std_train
        # test_X = hstack((test_X, ones(test_X.shape[0]).reshape(-1,1)))
        #
        # reg = linear_model.Lasso(alpha=.01, fit_intercept=False, max_iter=1000,
        #                         normalize=False, tol=0.00001)
        # reg.fit(train_X,train_y)
        # # intercept
        # print(-(mean_train/std_train * reg.coef_[:-1]).sum() + reg.coef_[-1])
        # for k,v in zip(keys, reg.coef_[:-1] / std_train):
        #     print('{:30} {}'.format(k,v))
        # print(reg.coef_[:-1] / std_train)
        # print(reg.score(test_X, test_y))
        # print(reg.predict(test_X[0:5]))

        # we count how many positive examples we are using to adjust the lambda
        # so that it corresponds to scikit learn's alpha
        n = mldb.get('/v1/query',
                     q="""
                    select count(*)
                    from wine_red
                    where implicit_cast(rowName()) % 2 = 0
                    """).json()[0]['columns'][0][1]

        # mldb.put('/v1/procedures/patate', {
        #     'type': 'transform',
        #     'params': {
        #         'inputData': 'select * EXCLUDING (quality), quality - 5.67125 as quality'
        #                      ' from wine_red',
        #         'outputDataset': 'wine_red_norm',
        #         'runOnCreation': True
        #     }
        # })

        modelFileUrl = 'file://tmp/MLDB-174-wine.cls'
        config = {
                "type": "classifier.experiment",
                "params": {
                    "inputData": """
                        select
                        {* EXCLUDING (quality)} as features,
                        quality as label
                        from wine_red
                        order by implicit_cast(rowName()) -- to compare with scikit learn
                    """,
                    "datasetFolds": [
                        {
                            "trainingWhere": "implicit_cast(rowName()) % 2 = 0",
                            "testingWhere": "implicit_cast(rowName()) % 2 = 1"
                        }
                    ],
                    "experimentName": "winer",
                    "modelFileUrlPattern": modelFileUrl,
                    "algorithm": "glz_l1",
                    "configuration": {
                        "glz_l1": {
                            "type": "glz",
                            "verbosity": 3,
                            "normalize": True,
                            "link_function": 'linear',
                            "regularization": 'l1',
                            "max_regularization_iteration": 1000,
                            # times 2 * n to match sklearn's lambda
                            "regularization_factor": 0.01 * (2. * n)
                        }
                    },
                    "mode": "regression",
                    "runOnCreation": True
                }
            }
        # the training should now work
        rez = mldb.put('/v1/procedures/wine_trainer', config)

        # check the performance is in the expected range
        perf = rez.json()["status"]["firstRun"]["status"] \
                         ["folds"][0]["resultsTest"]["r2"]
        mldb.log(perf)
        self.assertAlmostEqual(perf, 0.362, places=2)

        # make sure the trained model used all features
        scorerDetails = mldb.get("/v1/functions/winer_scorer_0/details").json()
        mldb.log(scorerDetails)
        usedFeatures = [x["feature"]
                        for x in scorerDetails["model"]["params"]["features"]]
        mldb.log(usedFeatures)
        self.assertGreater(len(usedFeatures), 2)

        functionConfig = {
            "type": "classifier",
            "params": {
                "modelFileUrl": modelFileUrl
            }
        }
        createFunctionOutput = mldb.put("/v1/functions/regressor_l1",
                                        functionConfig)
        mldb.log(createFunctionOutput.json())

        # compare the weights of the model with the weights from sciki learn
        details = mldb.get('/v1/functions/regressor_l1/details').json()
        weights = details['model']['params']['weights'][0][:-1]
        weights_scikit_learn = [
            -2.74455747e-03, -3.25523337e-01, 3.53340841e-03,  -1.55484223e+00,
            1.20525662e-03, -0.00000000e+00, -8.56879480e-01, -2.56506702e-03,
            2.63893600e-01, 0.00000000e+00, 9.06624432e-01]
        mldb.log(weights)
        self.assertEqual(len(weights), len(weights_scikit_learn))
        for a,b in zip(weights, weights_scikit_learn):
            self.assertAlmostEqual(a, b, delta=1e-3)
        # TODO add the bias when it's in there (MLDBFB-535)

        vals = mldb.get('/v1/query', q="""
            SELECT regressor_l1({features: {* EXCLUDING (quality)}}) as *
            FROM wine_red
            WHERE implicit_cast(rowName()) % 2 = 1
            ORDER BY implicit_cast(rowName())
            LIMIT 5""", format='soa').json()['score']
        mldb.log(vals)
        vals_sklearn = [5.19923122, 5.66831682, 5.14151357, 5.34206547,
                        5.67785202]
        for a,b in zip(vals, vals_sklearn):
            self.assertAlmostEqual(a, b, delta=1e-3)


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
                from wine_red
                where rowHash() % 2 = 1
                limit 2
        """)

        self.assertEqual(len(explain_rez), 3)

    def test_wine_quality_merged_regression_dt(self):
        def getConfig(dataset):
            return {
                "type": "classifier.experiment",
                "params": {
                    "inputData": """
                        select
                        {* EXCLUDING(quality)} as features,
                        quality as label
                        from %s
                    """ % dataset,
                    "datasetFolds": [
                            {
                                "trainingWhere": "rowHash() % 2 = 0",
                                "testingWhere": "rowHash() % 2 = 1"
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
        self.assertAlmostEqual(rez.json()["status"]["firstRun"]["status"]["folds"][0]["resultsTest"]["r2"], 0.28, delta=0.1)

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


    def test_simple_regression_explain_sum(self):

        modelFileUrl = "file://tmp/MLDB-174-sum-explain.cls"

        for cls in ["dt", "glz"]:
            clsProcConf = {
                "type": "classifier.train",
                "params": {
                    "trainingData": {
                        "select": "{x, y} as features, label as label",
                        "from": { "id": "test2" }
                    },
                    "configuration": self.cls_conf,
                    "algorithm": cls,
                    "modelFileUrl": modelFileUrl,
                    "mode": "regression",
                    "runOnCreation": True
                }
            }
            mldb.put("/v1/procedures/cls_train", clsProcConf)

            functionConfig = {
                "type": "classifier.explain",
                "params": {
                    "modelFileUrl": modelFileUrl
                }
            }
            mldb.put("/v1/functions/explain_dt_test2", functionConfig)

            functionConfig = {
                "type": "classifier",
                "params": {
                    "modelFileUrl": modelFileUrl
                }
            }
            mldb.put("/v1/functions/cls_dt_test2", functionConfig)

            explain_rez = mldb.query("""
                    select explain_dt_test2({{y, x} as features,
                                      label as label}) as explain,
                            cls_dt_test2({{y, x} as features}) as score,
                            label as label
                    from test2
            """)

            mldb.log(explain_rez)

            # make sure that summing up all explain values gives us the prediction
            for line in explain_rez[1:]:
                self.assertAlmostEqual(sum((x for x in line[1:4] if x != None)), line[5], places=5)

    def test_mldb_1712_failure_on_non_matching_features(self):
        model_file_url = "file://tmp/MLDB-1712-sum-explain.cls"

        mldb.put("/v1/procedures/cls_train_mldb_1712", {
            "type": "classifier.train",
            "params": {
                "trainingData": {
                    "select": "{x, y} as features, label as label",
                    "from": { "id": "test2" }
                },
                "configuration": self.cls_conf,
                "algorithm": 'glz',
                "modelFileUrl": model_file_url,
                "mode": "regression",
                "runOnCreation": True
            }
        })

        mldb.put("/v1/functions/explain_dt_mldb_1712", {
            "type": "classifier.explain",
            "params": {
                "modelFileUrl": model_file_url
            }
        })


        msg = "The specified features couldn't be found in the classifier."
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("""
                SELECT explain_dt_mldb_1712({{octosanchez} AS features,
                                              label AS label}) AS explain,
                        label AS label
                FROM test2
            """)


    def test_r2(self):
        ds = mldb.create_dataset({ "id": "r2_sample", "type": "sparse.mutable" })
        ds.record_row("a",[["score", 2.5, 0], ["score2", 25, 0], ["target", 3, 0]])
        ds.record_row("b",[["score", 0, 0], ["score2", -5, 0], ["target", -0.5, 0]])
        ds.record_row("c",[["score", 2, 0], ["score2", 22, 0], ["target", 2, 0]])
        ds.record_row("d",[["score", 8, 0], ["score2", 5, 0], ["target", 7, 0]])
        ds.commit()

        for scoreCol, r2 in [("score", 0.948), ("score2", -30.1177)]:
            rez = mldb.put("/v1/procedures/patate", {
                "type": "classifier.test",
                "params": {
                    "testingData": "select %s as score, target as label from r2_sample" % scoreCol,
                    "mode": "regression",
                    "runOnCreation": True
                }
            })

            mldb.log(rez.json()["status"])
            self.assertAlmostEqual(rez.json()["status"]["firstRun"]["status"]["r2"], r2, places=2)

    def test_r2_edge(self):
        ds = mldb.create_dataset({ "id": "r2_sample_edge", "type": "sparse.mutable" })
        ds.record_row("a",[["score", 1, 0], ["score2", 2, 0], ["target", 1, 0], ["target2", 1, 0]])
        ds.record_row("b",[["score", 1, 0], ["score2", 1, 0], ["target", 1, 0], ["target2", 2, 0]])
        ds.record_row("c",[["score", 1, 0], ["score2", 1, 0], ["target", 1, 0], ["target2", 1, 0]])
        ds.commit()

        for scoreCol, targetCol, r2 in [("score", "target", 1), ("score2", "target", 0), ("score", "target2", -0.5)]:
            rez = mldb.put("/v1/procedures/patate", {
                "type": "classifier.test",
                "params": {
                    "testingData": "select %s as score, %s as label from r2_sample_edge" % (scoreCol, targetCol),
                    "mode": "regression",
                    "runOnCreation": True
                }
            })

            mldb.log(rez.json()["status"])
            self.assertAlmostEqual(rez.json()["status"]["firstRun"]["status"]["r2"], r2, places=2)


mldb.run_tests()

