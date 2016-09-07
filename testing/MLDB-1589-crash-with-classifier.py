#
# MLDB-1589-crash-with-classifier.py
# Guy Dumais, 2016-09-02
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1589CrashWithClassifier(MldbUnitTest):  # noqa

    def test_crash_in_classifier(self):
        # load a smaller version of the training data
        # it seems that the dataset has to be imported
        # may be because row need to be in a certain order
        mldb.put('/v1/procedures/import_bench_train_1m', {
            "type": "import.text",
            "params": { 
                "dataFileUrl": "file://mldb/testing/dataset/MLDB-1589-data.csv",
                "outputDataset":"bench_train_1m",
                "limit": 5,
                "runOnCreation": True
            }
        })

        mldb.put('/v1/procedures/import_bench_test', {
            "type": "import.text",
            "params": { 
                "dataFileUrl": "file://mldb/testing/dataset/MLDB-1589-data.csv",
                "outputDataset":"bench_test",
                "limit": 1,
                "runOnCreation": True
            }
        })

        mldb.log(mldb.query("select * from bench_train_1m"))

        # this training was crashing MDLB
        mldb.put('/v1/procedures/benchmark', {
            "type": "classifier.experiment",
            "params": {
                "experimentName": "sample",
                "inputData": """
                    select {
                           lower(UniqueCarrier)
                          } as features,
                          dep_delayed_15min = 'Y' as label
                    from bench_train_1m 
                """, 
                "testingDataOverride":	"""
                    select {
                           lower(UniqueCarrier)
                          } as features,
                          dep_delayed_15min = 'Y' as label
                    from bench_test
                """,
                "configuration": {
                    "type": "bagging",
                    "num_bags": 100,
                    "validation_split": 0,
                    "weak_learner": {
                        "type": "decision_tree",
                        "max_depth": 20,
                        "random_feature_propn": 0.3
                    }
                },
                "modelFileUrlPattern": "file://tmp/benchml_$runid.cls",	    
                "mode": "boolean"
            }
        })


if __name__ == '__main__':
    mldb.run_tests()
