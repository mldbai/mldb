#
# MLDB-xxx-explain.py
# Mich, 2016-12-07
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldbfb724ClassifierExpSegfaultTest(MldbUnitTest):  # noqa

    def test_it(self):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.commit()
        ds = mldb.create_dataset({'id' : '_inception', 'type' : 'sparse.mutable'})
        ds.commit()
#
        query = """SELECT {_inception.* EXCLUDING(image_url)} AS features,
                 ds.cei AS label
                 FROM _inception
                 INNER JOIN ds ON _inception.image_url=ds.image_url"""
        mldb.log(query)
        mldb.log(mldb.query(query))
        mldb.put_async("/v1/procedures/trainer", {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "exp_",
                "mode": "boolean",
                "inputData": query,
                'datasetFolds': [{'trainingWhere': 'rowHash() % 10 != 0',
                                'testingWhere': 'rowHash() % 10 = 0'}],
                "algorithm": "my_bbdt",
                "configuration": {
                    "my_bbdt": {
                        "type": "bagging",
                        "verbosity": 3,
                        "weak_learner": {
                            "type": "boosting",
                            "verbosity": 3,
                            "weak_learner": {
                                "type": "decision_tree",
                                "verbosity": 0,
                                "max_depth": 10,
                                "random_feature_propn": 1
                            },
                            "min_iter": 5,
                            "max_iter": 30
                        },
                        "num_bags": 1
                    }
                },
                'modelFileUrlPattern': 'file://$runid.cls',
                'evalTrain': False
            }
        })

if __name__ == '__main__':
    mldb.run_tests()
