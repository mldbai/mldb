#
# MLDB-2095-boosting-and-stump-issue.py
# datacratic, 2016
# this file is part of mldb. copyright 2016 datacratic. all rights reserved.
#
import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

class MLDB1907ValueDescriptionError(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'test',
        }

        dataset = mldb.create_dataset(dataset_config)

        ts = datetime.datetime.now()

        def record_example(row, label, x, y):
            # all variables are dummy except variable y that can perfectly
            # separate the data based upon whether it's missing or not
            columns = [["label", label, ts], 
                       ["a1", 2, ts], 
                       ["a3", 2, ts], 
                       ["a2", 2, ts], 
                       ["a4", 2, ts], 
                       ["a5", 2, ts], 
                       ["x", x, ts]]
            
            dataset.record_row(row, columns if y is None else columns + [["y", 1, ts ]])

        record_example("exf1", 0, 1, None)
        record_example("exf2", 0, 3, None)
        record_example("exf3", 0, 5, None)
        record_example("exf4", 0, 7, None)
        record_example("exf5", 0, 9, None)

        record_example("ext1", 1, 2,   1)
        record_example("ext2", 1, 4,   1)
        record_example("ext3", 1, 6,   1)
        record_example("ext4", 1, 8,   1)
        record_example("ext5", 1, 10,  1)

        dataset.commit()

    def train_classifier(self, features):
        modelFileUrl = "file://tmp/MLDB-2095-decision-tree.cls"

        trainClassifierProcedureConfig = {
            "type": "classifier.train",
            "params": {
                "trainingData": "select " + features + " as features, label from test",
                "configuration": {
                    "boosting_and_stumps": {
                        "type": "boosting",
                        "min_iter": 1,
                        "max_iter": 1,
                        "weak_learner": {
                            "type": "stump",
                            "verbosity": 3,
                            "trace": 5,
                            "update_alg": "prob"
                        }
                    }
                },
            "algorithm": "boosting_and_stumps",
            "modelFileUrl": modelFileUrl,
            "equalizationFactor": 0.0,
            "mode": "boolean",
            "functionName": "classifier"
            }
        }

        res = mldb.put("/v1/procedures/cls_train", trainClassifierProcedureConfig)

    @unittest.expectedFailure #MLDB-2095
    def test_feature_ordering_odd(self):
        self.train_classifier("{a1,a2,a3,a4,a5,x,y}")

        model = mldb.get("/v1/functions/classifier/details")
        mldb.log(model)

        # Applying it with y present should return 1
        result1 = mldb.get("/v1/functions/classifier/application",
                           { "input": { "features": { 'a':0, 'y':1, 'z':0 }}}).json()
        mldb.log(result1)
        self.assertEqual(result1['output']['score'], 1);

        # Applying it with y missing should return 0
        result2 = mldb.get("/v1/functions/classifier/application",
                           { "input": { "features": { 'q':1 }}}).json()
        mldb.log(result2)
        self.assertEqual(result2['output']['score'], 0);

    def test_feature_ordering_even(self):
        self.train_classifier("{a1,a2,a3,a4,x,y}")

        model = mldb.get("/v1/functions/classifier/details")
        mldb.log(model)

        # Applying it with y present should return 1
        result1 = mldb.get("/v1/functions/classifier/application",
                           { "input": { "features": { 'a':0, 'y':1, 'z':0 }}}).json()
        mldb.log(result1)
        self.assertEqual(result1['output']['score'], 1);

        # Applying it with y missing should return 0
        result2 = mldb.get("/v1/functions/classifier/application",
                           { "input": { "features": { 'q':1 }}}).json()
        mldb.log(result2)
        self.assertEqual(result2['output']['score'], 0);

   
if __name__ == '__main__':
    mldb.run_tests()
