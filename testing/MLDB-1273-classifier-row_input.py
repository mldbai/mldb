#
# MLDB-1273-classifier-row_input.py
# 2015
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

import unittest


class ClassifierRowInputTest(unittest.TestCase):

    def test_it(self):
        mldb.create_dataset({
            "id": "iris",
            "type": "text.csv.tabular",
            "params": {
                "dataFileUrl": "file://mldb/testing/dataset/iris.data",
                "headers": ["a", "b", "c", "d", "class"]
            }
        })

        mldb.put("/v1/functions/feats", {
            'type' : 'sql.expression',
            'params' : {
                "expression": "{a,b,c,d} as row"
            }
        })

        mldb.put("/v1/procedures/create_train_set", {
            'type' : 'transform',
            'params' : {
                "inputData": "select feats({*}) as *, "
                             "class='Iris-setosa' as label from iris",
                "outputDataset": "train_set",
                "runOnCreation": True
            }
        })

        mldb.put("/v1/procedures/train", {
            'type' : 'classifier.train',
            'params' : {
                'trainingData' : """
                    select
                        {* EXCLUDING(label)} as features,
                        label
                    from train_set
                """,
                "modelFileUrl": "file://tmp/MLDB-1273.cls",
                "configuration": {
                    "dt": {
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 3,
                        "update_alg": "prob"
                    },
                },
                "algorithm": "dt",
                "functionName": "cls",
                "runOnCreation": True
            }
        })

        with_flattening = mldb.query("""
            select cls({features: {
                a as "row.a", b as "row.b", c as "row.c", d as "row.d"
                }}) as *
            from iris
            limit 10
        """)

        without_flattening = mldb.query("""
            select cls({features: {feats({*}) as *}}) as *
            from iris
            limit 10
        """)

        self.assertEqual(with_flattening, without_flattening)

if __name__ == '__main__':
    mldb.run_tests()
