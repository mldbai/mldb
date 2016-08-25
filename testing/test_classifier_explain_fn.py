#
# test_classifier_explain_fn.py
# Simon Lemieux, 2016-08-25
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
# This tests the `classifier.explain` function.
#

import os, tempfile, pprint

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
        headers = ['x', 'y', 'label', 'weight']
        data = """
            -1 -1 0 1
            -1  1 0 3
             1 -1 0 3
             1  1 1 3
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
        headers = ['x', 'y', 'label', 'weight']
        data = """
            -1 -1 0 1
            -1 -1 0 1
            -1  1 0 3
             1 -1 0 3
             1  1 1 3
            """
        cls.make_dataset(headers, data, 'simple_and_hack')


    def test_dt_boolean_no_weight(self):
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

    def test_glz_boolean_no_weight(self):
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

    def test_dt_reg_no_weight(self):
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

        mldb.log(mldb.query("""
            SELECT explain({features: {x:0.7, y:1.2}, label:8}) AS *
        """))

        mldb.log(mldb.query("""
            SELECT explain({features: {x:0.7, y:1.2}, label:12}) AS *
        """))



if __name__ == '__main__':
    mldb.run_tests()
