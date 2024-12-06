#
# classifier_test_err_on_empty_sets.py
# Mich, 2016-06-07
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
from mldb import mldb, MldbUnitTest, ResponseException
import os

tmp_dir=os.getenv("TMP")
if tmp_dir[-1] != os.sep:
    tmp_dir += os.sep

class ClassifierTestErrorWhenNoDataTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['label', 1, 12], ['feat1', 1, 0], ['feat2', 1, 0]])
        ds.record_row('row2', [['label', 0, 12], ['feat1', 1, 0], ['feat2', 0, 0]])
        ds.record_row('row3', [['label', 0, 12], ['feat1', 0, 0], ['feat2', 0, 0]])
        ds.commit()

        mldb.post('/v1/procedures', {
            'type' : 'classifier.train',
            'params' : {
                'runOnCreation' : True,
                "mode": "boolean",
                'algorithm' : 'glz',
                "configuration": {
                    "glz": {
                        "type": "glz",
                        "verbosity": 3,
                        "normalize": False,
                        "regularization": 'l2'
                    }
                },
                'trainingData' : """
                    SELECT {* EXCLUDING(label)} AS features, label
                    FROM ds
                """,
                "modelFileUrl":
                    "file://" + tmp_dir + "fmlhTODO.cls",
            }
        })

    def test_classifier_test_no_data(self):
        err_str = "Cannot run classifier.test procedure on empty test set"
        with self.assertRaisesRegex(ResponseException, err_str):
            mldb.post('/v1/procedures', {
                "type": "classifier.test",
                "params": {
                    'runOnCreation' : True,
                    "testingData": """
                        SELECT
                            {* EXCLUDING(label)} AS features,
                            label AS score,
                            label AS label
                        FROM ds
                        LIMIT 0
                    """
                }
            })

        with self.assertRaisesRegex(ResponseException, err_str):
            mldb.post('/v1/procedures', {
                "type": "classifier.test",
                "params": {
                    'runOnCreation' : True,
                    "testingData": """
                        SELECT
                            {* EXCLUDING(label)} AS features,
                            label AS score,
                            label AS label
                        FROM ds
                        OFFSET 100
                    """
                }
            })

        with self.assertRaisesRegex(ResponseException, err_str):
            mldb.post('/v1/procedures', {
                "type": "classifier.test",
                "params": {
                    'runOnCreation' : True,
                    "testingData": """
                        SELECT
                            {* EXCLUDING(label)} AS features,
                            label AS score,
                            label AS label
                        FROM ds
                        WHERE patate=123
                    """
                }
            })

if __name__ == '__main__':
    mldb.run_tests()
