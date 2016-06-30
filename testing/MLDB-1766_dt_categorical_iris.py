#
# MLDB-1766_dt_categorical_iris.py
# Francois Maillet, 2016-06-30
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1766DtCategoricalIris(MldbUnitTest):  # noqa
    @classmethod
    def setUpClass(cls):
        mldb.put('/v1/procedures/import_iris', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "http://public.mldb.ai/iris.data",
                "headers": [ "sepal length", "sepal width", "petal length", "petal width", "class" ],
                "outputDataset": "iris",
                "runOnCreation": True
            }
        })

    def test_it(self):
        mldb.put('/v1/procedures/iris_train_classifier', {
            'type' : 'classifier.train',
            'params' : {
                'trainingData' : """
                    select 
                        {* EXCLUDING(class)} as features, 
                        class as label 
                    from iris 
                    where rowHash() % 5 = 0
                """,
                "algorithm": "dt",
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 3,
                        "update_alg": "prob"
                    },
                },
                "modelFileUrl": "file://models/iris.cls",
                "mode": "categorical",
                "functionName": "iris_classify",
                "runOnCreation": True
            }
        })

        rez = mldb.put('/v1/procedures/iris_test_classifier', {
            'type' : 'classifier.test',
            'params' : {
                'testingData' : """
                    select 
                        iris_classify({
                            features: {* EXCLUDING(class)}
                        }) as score,
                        class as label 
                    from iris 
                    where rowHash() % 5 != 0
                """,
                "mode": "categorical",
                "runOnCreation": True
            }
        })

        runResults = rez.json()["status"]["firstRun"]["status"]

        self.assertLess(runResults["labelStatistics"]["Iris-virginica"]["recall"], 0.9)

if __name__ == '__main__':
    mldb.run_tests()
