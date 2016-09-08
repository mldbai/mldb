#
# test_classifier_explain_fn.py
# Simon Lemieux, 2016-08-25
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
# This tests the `classifier.explain` function.
#

import os, tempfile, pprint, unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

def get_temporary_file():
    return tempfile.NamedTemporaryFile(prefix=os.getcwd() + '/build/x86_64/tmp')


class TestClassifierExplainFn(MldbUnitTest):  # noqa

    maxDiff = None

    def assert_recursive_almost_equal(self, a, b, places=7, path=None):
        if path is None:
            path = []
        for key, item_a in a.iteritems():
            path_key = path + [key]
            item_b = b[key]
            if isinstance(item_a, dict):
                self.assertTrue(isinstance(item_b, dict))
                self.assertEqual(len(item_a), len(item_b))
                self.assert_recursive_almost_equal(item_a, item_b, places,
                                                   path_key)
            else:
                # if item_a != item_b:
                #     mldb.log(path_key)
                #     mldb.log(item_a)
                #     mldb.log(item_b)
                self.assertAlmostEqual(item_a, item_b, places)

    @classmethod
    def make_dataset(self, headers, data, name):
        data = [map(float, row.strip().split())
                for row in data.strip().split('\n')]

        mldb.put('/v1/datasets/' + name, {
            'type': 'tabular'
        })
        for i, row in enumerate(data):
            mldb.post('/v1/datasets/{}/rows'.format(name), {
                'rowName': str(i),
                'columns': [[col,val,0] for col,val in zip(headers, row)]
            })
        mldb.post('/v1/datasets/{}/commit'.format(name))

        # mldb.log(mldb.query("""SELECT * FROM """ + name))

    @classmethod
    def setUpClass(cls):
        headers = ['x', 'y', 'label']
        data = """
            -1 -1 0
            -1  1 0
             1 -1 0
             1  1 1
            """
        cls.make_dataset(headers, data, 'simple_and')

        # headers = ['x', 'y', 'label', 'weight']
        # data = """
        #     -1 -1  0 0.5
        #     -1 -1  0 0.5
        #     -1  1  0 1
        #      1 -1  0 1
        #      1  1 10 1
        #     """
        # cls.make_dataset(headers, data, 'reg')
        # mldb.log(mldb.query("SELECT * FROM reg"))
        headers = ['x', 'y', 'label']
        data = """
            -1 -1 0
            -1  1 0
             1 -1 0
             1  1 1
            """
        cls.make_dataset(headers, data, 'simple_and_hack')

        # titanic data
        mldb.post("/v1/procedures", {
            "type": "import.text",
            "params": {
                'dataFileUrl' : "https://raw.githubusercontent.com/datacratic"
                                "/mldb-pytanic-plugin/master/titanic_train.csv",
                "outputDataset": {
                    "id": "titanic_raw",
                }
            }
        })


    def test_dt_bool(self):
        for update_alg in ['prob', 'gentle', 'normal']:
            mldb.log(update_alg)
            _model_file = get_temporary_file()
            model_file = 'file:///' + _model_file.name
            res = mldb.put('/v1/procedures/_', {
                'type': 'classifier.train',
                'params': {
                    'mode': 'boolean',
                    'trainingData': "SELECT features: {x,y}, label FROM simple_and",
                    'algorithm': 'dt',
                    'modelFileUrl': model_file,
                    'configuration': {
                        'dt': {
                            'type': 'decision_tree',
                            'max_depth': 2,
                            'update_alg': update_alg
                        }
                    },
                    'functionName': 'classify'
                }
            }).json()
            # mldb.log(res)
            mldb.log(pprint.pformat(mldb.get('/v1/functions/classify/details').json()))

            mldb.log(mldb.query("""
                SELECT classify({features: {x:0.7, y:1.2}}) AS *
            """))

            mldb.put('/v1/functions/explain', {
                'type': 'classifier.explain',
                'params': {
                    'modelFileUrl': model_file
                }
            })

            mldb.log(mldb.query("""
            SELECT explain({features: {x:0.7, y:1.2}, label:1}) AS *
            """))

    def test_glz_bool(self):
        _model_file = get_temporary_file()
        model_file = 'file:///' + _model_file.name
        res = mldb.put('/v1/procedures/_', {
            'type': 'classifier.train',
            'params': {
                'mode': 'boolean',
                'trainingData': "SELECT features: {x,y}, label FROM simple_and",
                'algorithm': 'glz',
                'modelFileUrl': model_file,
                'configuration': {
                    'glz': {
                        'type': 'glz',
                    }
                },
                'functionName': 'classify'
            }
        }).json()
        # mldb.log(res)
        mldb.log(pprint.pformat(mldb.get('/v1/functions/classify/details').json()))

        mldb.log(mldb.query("""
            SELECT classify({features: {x:0.7, y:1.2}}) AS *
        """))

        mldb.put('/v1/functions/explain', {
            'type': 'classifier.explain',
            'params': {
                'modelFileUrl': model_file
            }
        })

        mldb.log(mldb.query("""
            SELECT explain({features: {x:0.7, y:1.2}, label:1}) AS *
        """))

        mldb.log(mldb.query("""
            SELECT explain({features: {x:0.7, y:1.2}, label:0}) AS *
        """))

    def test_dt_reg(self):
        mldb.log('test dt reg no weight')
        _model_file = get_temporary_file()
        model_file = 'file:///' + _model_file.name
        res = mldb.put('/v1/procedures/_', {
            'type': 'classifier.train',
            'params': {
                'mode': 'regression',
                'trainingData': "SELECT features: {x,y}, label FROM simple_and_hack",
                'algorithm': 'dt',
                'modelFileUrl': model_file,
                'configuration': {
                    'dt': {
                        'type': 'decision_tree',
                        'max_depth': 2,
                    }
                },
                'functionName': 'classify'
            }
        }).json()
        mldb.log(res)
        mldb.log(pprint.pformat(mldb.get('/v1/functions/classify/details').json()))

        mldb.log(mldb.query("""
            SELECT classify({features: {x:0.7, y:1.2}}) AS *
        """))

        mldb.put('/v1/functions/explain', {
            'type': 'classifier.explain',
            'params': {
                'modelFileUrl': model_file
            }
        })

        for label in [1, .8, 1.2]:
            mldb.log(mldb.query("""
                SELECT explain({features: {x:0.7, y:1.2}, label:%f}) AS *
            """ % label))

    def test_dt_titanic(self):
        mldb.log("dt titanic")
        _model_file = get_temporary_file()
        model_file = 'file:///' + _model_file.name
        mldb.post("/v1/procedures", {
            "type": "classifier.train",
            "params": {
                "mode": "boolean",
                "trainingData": """
                    select
                        {Sex, Age, Fare, Embarked, Parch, SibSp, Pclass} as features,
                        label
                    from titanic_raw
                """,
                "algorithm": "dt",
                "configurationFile": "mldb/container_files/classifiers.json",
                "modelFileUrl": model_file,
                "functionName": "score"
            }
        })

        mldb.put("/v1/functions/dt_explainer", {
            "type": "classifier.explain",
            "params": {
                "modelFileUrl": model_file
            }
        })

        mldb.log(mldb.query("SELECT * FROM titanic_raw LIMIT 1"))

        # mldb.log(mldb.query("""
        #     select score({features: {
        #         Age: 22,
        #         Embarked: 'S',
        #         Fare: 7.25,
        #         Parch: 0,
        #         Pclass: 3,
        #         Sex: 'male',
        #         SibSp: 1
        # }}) AS *"""))

        mldb.log(mldb.query("""
            select score({features: {*}, label: 0}) as * from titanic_raw limit 1
        """))

        mldb.log(mldb.query("""
            select dt_explainer({features: {*}, label: 0}) as * from titanic_raw limit 1
        """))

    # @unittest.skip('not implemented yet')
    def test_naive_bayes_bool(self):
        _model_file = get_temporary_file()
        model_file = 'file:///' + _model_file.name
        mldb.post("/v1/procedures", {
            "type": "classifier.train",
            "params": {
                "mode": "boolean",
                "trainingData": """
                    select
                        {Sex, Age, Fare, Embarked, Parch, SibSp, Pclass} as features,
                        label
                    from titanic_raw
                """,
                "algorithm": "naive_bayes",
                "configurationFile": "mldb/container_files/classifiers.json",
                "modelFileUrl": model_file,
                "functionName": "nbf"
            }
        })

        mldb.log('super naive bayes')
        mldb.log(pprint.pformat(mldb.get('/v1/functions/nbf/details').json()))

        mldb.put("/v1/functions/naive_bayes_explainer", {
            "type": "classifier.explain",
            "params": {
                "modelFileUrl": model_file
            }
        })

        # should throw a nice error saying we need a label
        with self.assertRaises(mldb_wrapper.ResponseException):  # noqa
            mldb.get('/v1/query', q="""
                select naive_bayes_explainer({features: {*}}) as * from titanic_raw limit 1
            """)

        mldb.log(mldb.query("""
            select naive_bayes_explainer({features: {*}, label: 0}) as * from titanic_raw limit 1
        """))

    def test_boosted_stumps_bool(self):
        _model_file = get_temporary_file()
        model_file = 'file:///' + _model_file.name
        mldb.post("/v1/procedures", {
            "type": "classifier.train",
            "params": {
                "mode": "boolean",
                "trainingData": """
                    select
                        {Sex, Age, Fare, Embarked, Parch, SibSp, Pclass} as features,
                        label
                    from titanic_raw
                """,
                "algorithm": "bs",
                "configurationFile": "mldb/container_files/classifiers.json",
                "modelFileUrl": model_file
            }
        })

        mldb.put("/v1/functions/bs_explainer", {
            "type": "classifier.explain",
            "params": {
                "modelFileUrl": model_file
            }
        })

        with self.assertRaises(mldb_wrapper.ResponseException):  # noqa
            mldb.get('/v1/query', q="""
                select bs_explainer({features: {*}}) as * from titanic_raw limit 1
            """)

        mldb.log(mldb.query("""
            select bs_explainer({features: {*}, label: 0}) as * from titanic_raw limit 1
        """))


if __name__ == '__main__':
    mldb.run_tests()
