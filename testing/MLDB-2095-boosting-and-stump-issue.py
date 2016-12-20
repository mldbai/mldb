#
# MLDB-2095-boosting-and-stump-issue.py
# datacratic, 2016
# this file is part of mldb. copyright 2016 datacratic. all rights reserved.
#
import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

class MLDB2095BoostingOnStumpError(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : 'test',
        }

        dataset = mldb.create_dataset(dataset_config)

        ts = datetime.datetime.now()

        def record_example(row, label, x, y):
            # variable x can perfectly separate the data based upon whether 
            # it's missing or not, variable y is dummy
            header = [["label", label, ts]]
                      
            dataset.record_row(row, header + [["y", y, ts]] if x is None 
                               else header + [["x", x, ts ]] + [["y", y, ts]])

        record_example("exf1", 0, None, 1)
        record_example("exf2", 0, None, 3)
        record_example("exf3", 0, None, 5)
        record_example("exf4", 0, None, 7)
        record_example("exf5", 0, None, 9)

        record_example("ext1", 1, 1, 2)
        record_example("ext2", 1, 1, 4)
        record_example("ext3", 1, 1, 6)
        record_example("ext4", 1, 1, 8)
        record_example("ext5", 1, 1, 10)

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
                    },
                    "decision_tree" : {
                        "type": "decision_tree",
                        "max_depth": 1,
                        "verbosity": 3,
                        "update_alg": "prob"
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

    def check_result(self):
        model = mldb.get("/v1/functions/classifier/details")
        mldb.log(model)

        # Applying it with x present should return 1
        result1 = mldb.get("/v1/functions/classifier/application",
                           { "input": { "features": { 'x':1 }}}).json()
        mldb.log(result1)
        self.assertEqual(result1['output']['score'], 1);

        # Applying it with x missing should return 0
        result2 = mldb.get("/v1/functions/classifier/application",
                           { "input": { "features": { 'q':1 }}}).json()
        mldb.log(result2)
        self.assertEqual(result2['output']['score'], 0);

  #  def test_feature_ordering_odd(self):
  #      self.train_classifier("{x}")
  #      self.check_result()

    def test_feature_ordering_even(self):
        self.train_classifier("{x,y}")
        self.check_result()
   
if __name__ == '__main__':
    mldb.run_tests()
