#
# MLDB-1273-classifier-row_input.py
# 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

mldb = mldb_wrapper.wrap(mldb) # noqa

import unittest


class ClassifierRowInputTest(unittest.TestCase):

    def test_it(self):

        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'file://mldb/testing/dataset/iris.data',
                "outputDataset": {
                    "id": "iris",
                },
                "runOnCreation": True,
                "headers": ["a", "b", "c", "d", "class"]
            }
        }
        mldb.put("/v1/procedures/csv_proc", csv_conf) 

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
                a as row.a, b as row.b, c as row.c, d as row.d
                }}) as *
            from iris
            limit 10
        """)

        without_flattening = mldb.query("""
            select cls({features: {feats({*}) as *}}) as *
            from iris
            limit 10
        """)
        #mldb.log(with_flattening)
        #mldb.log(without_flattening)

        self.assertEqual(with_flattening, without_flattening)

        with_aliasing = mldb.query("""
            select cls({features: {{a,b,c,d} as row}}) as *
            from iris
            limit 10
        """)

        #mldb.log(with_aliasing)
        self.assertEqual(with_flattening, with_aliasing,
                         "results do not match")

        with_aliasing = mldb.query("""
            select cls({features: {* as row.*}}) as *
            from iris
            limit 10
        """)

        self.assertEqual(with_flattening, with_aliasing, "results do not match")

        without_aliasing = mldb.query("""
            select cls({features: feats({*}) }) as *
            from iris
            limit 10
        """)

        self.assertEqual(with_flattening, without_aliasing,
                         "results do not match")

        mldb.put("/v1/functions/feats2", {
            'type' : 'sql.expression',
            'params' : {
                "expression": "feats({*}) as features"
            }
        })

        # MLDB-1314 
        without_name =  mldb.query("""
                        select cls( feats2({*}) ) as *
                        from iris
                        limit 10
                        """)
        mldb.log(without_name)
        self.assertEqual(with_flattening,without_name, "results do not match")

if __name__ == '__main__':
    mldb.run_tests()
