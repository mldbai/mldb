#
# test_classifier_test_proc.py
# Simon Lemieux, 2016-08-18
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
# This tests the `classifier.test` procedure.
#
from __future__ import division

mldb = mldb_wrapper.wrap(mldb)  # noqa


class TestClassifierTestProc(MldbUnitTest):  # noqa

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
            # mldb.log(row)
            mldb.post('/v1/datasets/{}/rows'.format(name), {
                'rowName': str(i),
                'columns': [[col,val,0] for col,val in zip(headers, row)]
            })
        mldb.post('/v1/datasets/{}/commit'.format(name))

    @classmethod
    def setUpClass(cls):
        # input dataset for classifier.test
        headers = ['score', 'bool_label', 'reg_label', 'weight']
        data = """
            1 0 10 1
            1 0 10 3
            2 1 20 3
            3 1 40 1
            """
        cls.make_dataset(headers, data, 'ds')

        headers = ['label', 'score.0', 'score.1', 'score.2', 'weight']
        data = """
            0  1 0 0  1
            1  0 1 0  3
            2  1 0 0  3
            2  0 0 1  1
            """
        cls.make_dataset(headers, data, 'cat')

        ds = mldb.create_dataset({'id' : 'foo', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['colA', 1, 1], ['label', 1, 1]])
        ds.record_row('row2', [['colA', 0, 1], ['label', 0, 1]])
        ds.commit()

    def _get_params(self, mode, label, weight):
        return {
            'type': 'classifier.test',
            'params': {
                'testingData': "SELECT score, label:{}, weight:{} FROM ds".format(label, weight),
                'outputDataset': 'out',
                'mode': mode
            }
        }

    def test_boolean_dataset_no_weight(self):
        res = mldb.post('/v1/procedures', self._get_params('boolean', 'bool_label', '1')).json()

        res = mldb.get('/v1/query', q="""
            SELECT * FROM out
            ORDER BY score DESC, implicit_cast(rowName()) DESC
            """,
            format='soa', rowNames=0).json()

        truth = {
            "index": [1, 2, 3, 3],
            "weight": [1, 1, 1, 1],
            "label": [1, 1, 0, 0],
            "score": [3, 2, 1, 1],

            "truePositives": [1, 2, 2, 2],
            "falseNegatives": [1, 0, 0, 0],
            "truePositiveRate": [0.5, 1, 1, 1],

            "trueNegatives": [2, 2, 0, 0],
            "falsePositives": [0, 0, 2, 2],
            "falsePositiveRate": [0, 0, 1, 1],

            "accuracy": [0.75, 1, 0.5, 0.5],
            "recall": [0.5, 1, 1, 1],
            "precision": [1, 1, 0.5, 0.5],
        }
        self.assertEqual(res, truth)

    def test_boolean_dataset_weight(self):
        mldb.post('/v1/procedures', self._get_params('boolean', 'bool_label', 'weight'))
        res = mldb.get('/v1/query', q="""
            SELECT * FROM out
            ORDER BY score DESC, implicit_cast(rowName()) DESC
            """,
            format='soa', rowNames=0).json()

        truth = {
            "index": [1, 2, 3, 3],
            "weight": [1, 3, 3, 1],
            "label": [1, 1, 0, 0],
            "score": [3, 2, 1, 1],

            "truePositives": [1, 4, 4, 4],
            "falseNegatives": [3, 0, 0, 0],

            "truePositiveRate": [0.25, 1, 1, 1],

            "trueNegatives": [4, 4, 0, 0],
            "falsePositives": [0, 0, 4, 4],

            "falsePositiveRate": [0, 0, 1, 1],

            "accuracy": [5/8, 1, 0.5, 0.5],
            "recall": [0.25, 1, 1, 1],
            "precision": [1, 1, 0.5, 0.5],
        }
        self.assertEqual(res, truth)

    def test_regression_no_weight(self):
        res = mldb.post('/v1/procedures',
                        self._get_params('regression', 'reg_label', '1')
                        ).json()['status']['firstRun']['status']
        # mldb.log(res)
        # https://en.wikipedia.org/wiki/Coefficient_of_determination#Definitions
        y_mean = (10 + 10 + 20 + 40) / 4
        ss_tot = (10 - y_mean)**2  * 2 + (20 - y_mean)**2 + (40 - y_mean)**2
        ss_res = (10 - 1)**2  * 2 + (20 - 2)**2 + (40 - 3)**2

        # using the same quantile definition as in our code
        rel_errs = sorted([(10-1)/10, (10-1)/10, (20-2)/20, (40-3)/40])

        truth = {
            "quantileErrors" : {
                "0.9": rel_errs[int(len(rel_errs)*.9-1)],
                "0.75": rel_errs[int(len(rel_errs)*.75-1)],
                "0.5": rel_errs[int(len(rel_errs)*.5-1)],
                "0.25": rel_errs[int(len(rel_errs)*.25-1)]
            },
            "mse": ((1-10)**2 * 2 + (2-20)**2 + (3-40)**2)/4,
            "r2": 1 - ss_res / ss_tot
        }
        self.assertEqual(res, truth)

    def test_regression_weight(self):
        res = mldb.post('/v1/procedures',
                        self._get_params('regression', 'reg_label', 'weight')
                        ).json()['status']['firstRun']['status']
        # mldb.log(res)
        # https://en.wikipedia.org/wiki/Coefficient_of_determination#Definitions
        y_mean = (10 + 10 * 3 + 20 * 3 + 40) / 8
        ss_tot = (10 - y_mean)**2  * 4 + (20 - y_mean)**2 * 3 + (40 - y_mean)**2
        ss_res = (10 - 1)**2  * 4 + (20 - 2)**2 * 3 + (40 - 3)**2

        # using the same quantile definition as in our code
        rel_errs = sorted([(10-1)/10, (10-1)/10, (20-2)/20, (40-3)/40])

        truth = {
            "quantileErrors" : {
                "0.9": rel_errs[int(len(rel_errs)*.9-1)],
                "0.75": rel_errs[int(len(rel_errs)*.75-1)],
                "0.5": rel_errs[int(len(rel_errs)*.5-1)],
                "0.25": rel_errs[int(len(rel_errs)*.25-1)]
            },
            "mse": ((1-10)**2 * 4 + (2-20)**2 *3 + (3-40)**2)/8,
            "r2": 1 - ss_res / ss_tot
        }
        self.assert_recursive_almost_equal(res, truth, places=10)

    def test_categorical_no_weight(self):
        res = mldb.post('/v1/procedures', {
            'type': 'classifier.test',
            'params': {
                'testingData': "SELECT label, score FROM cat",
                'outputDataset': 'out',
                'mode': 'categorical'
            }
        }).json()['status']['firstRun']['status']
        # mldb.log(res)
        conf = res.pop('confusionMatrix')

        truth = {
            'labelStatistics': {
                '0': {
                    "f1Score": 2/3,
                    "recall": 1,
                    "support": 1,
                    "precision": .5,
                    "accuracy": 0.75,
                },
                '1': {
                    "f1Score": 1,
                    "recall": 1,
                    "support": 1,
                    "precision": 1,
                    "accuracy": 1,
                },
                '2': {
                    "f1Score": 2. * .5  / 1.5,
                    "recall": 0.5,
                    "support": 2,
                    "precision": 1,
                    "accuracy": 0.75,
                },
            },
            'weightedStatistics': {
                'f1Score': (2/3 + 1 + 2/3*2)/4,
                'recall': (1+1+.5*2)/4,
                'support': 4,
                'precision': (.5+1+1*2)/4,
                'accuracy': (.75+1+.75*2)/4
            },
        }
        self.assertEqual(res, truth)

        truth_conf = [
                {'predicted': 0, 'actual': 0, 'count': 1},
                {'predicted': 0, 'actual': 2, 'count': 1},
                {'predicted': 1, 'actual': 1, 'count': 1},
                {'predicted': 2, 'actual': 2, 'count': 1},
            ]

        conf.sort(key=lambda x: (x['predicted'], x['actual']))

        self.assertEqual(truth_conf, conf)

    def test_categorical_weight(self):
        res = mldb.post('/v1/procedures', {
            'type': 'classifier.test',
            'params': {
                'testingData': "SELECT label, score, weight FROM cat",
                'outputDataset': 'out',
                'mode': 'categorical'
            }
        }).json()['status']['firstRun']['status']
        # mldb.log(res)
        conf = res.pop('confusionMatrix')

        truth = {
            'labelStatistics': {
                '0': {
                    "f1Score": 2/5,
                    "recall": 1,
                    "support": 1,
                    "precision": .25,
                    "accuracy": 5/8,
                },
                '1': {
                    "f1Score": 1,
                    "recall": 1,
                    "support": 3,
                    "precision": 1,
                    "accuracy": 1,
                },
                '2': {
                    "f1Score": 2/5,
                    "recall": 0.25,
                    "support": 4,
                    "precision": 1,
                    "accuracy": 5/8,
                },
            },
            'weightedStatistics': {
                'f1Score': (2/5 + 3 + 2/5*4) / 8,
                'recall': (1 + 3 + .25*4)/8,
                'support': 8,
                'precision': (.25 + 3 + 4) / 8,
                'accuracy': (5/8 + 3 + 5/8*4) / 8
            },
        }
        self.assertEqual(res, truth)

        truth_conf = [
                {'predicted': 0, 'actual': 0, 'count': 1},
                {'predicted': 0, 'actual': 2, 'count': 3},
                {'predicted': 1, 'actual': 1, 'count': 3},
                {'predicted': 2, 'actual': 2, 'count': 1},
            ]

        conf.sort(key=lambda x: (x['predicted'], x['actual']))

        self.assertEqual(truth_conf, conf)

    def test_classifier_rejects_unknown_params(self):
        msg = "Unknown key\\(s\\) encountered in config: glz_linear.FOO"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put("/v1/procedures/regression_cls", {
                "type": "classifier.experiment",
                "params": {
                    "inputData": "SELECT {*} AS features, label AS label FROM foo",
                    "experimentName": "reg_exp",
                    "keepArtifacts": True,
                    "modelFileUrlPattern": "file://temp/mldb-256_reg.cls",
                    "algorithm": "glz_linear",
                    "configuration": {
                        "glz_linear": {
                            "type": "glz",
                            "normalize": True,
                            "link_function": 'linear',
                            "regularization": 'l1',
                            "regularization_factor": 0.001,
                            "FOO": "linear"
                        }
                    },
                    "kfold": 2,
                    "mode": "regression",
                    "outputAccuracyDataset": True
                }
            })

    @unittest.expectedFailure
    def test_classifier_doesnt_rejects_dotted_neighbor_param(self):

        # This assertion works
        msg = "No feature vectors were produced as all"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put("/v1/procedures/regression_cls", {
                "type": "classifier.experiment",
                "params": {
                    "inputData": "SELECT {*} AS features, label AS label FROM foo",
                    "experimentName": "reg_exp",
                    "keepArtifacts": True,
                    "modelFileUrlPattern": "file://temp/mldb-256_reg.cls",
                    "algorithm": "glz_linear",
                    "configuration": {
                        "glz_linear": {
                            "type": "glz",
                            "normalize": True,
                            "link_function": 'linear',
                            "regularization": 'l1',
                            "regularization_factor": 0.001
                        },

                        # The key is clearly different, no problem
                        "FOO": {
                            "FOO": "linear"
                        }
                    },
                    "kfold": 2,
                    "mode": "regression",
                    "outputAccuracyDataset": True
                }
            })

        # This assertion fails, see below
        msg = "No feature vectors were produced as all"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put("/v1/procedures/regression_cls", {
                "type": "classifier.experiment",
                "params": {
                    "inputData": "SELECT {*} AS features, label AS label FROM foo",
                    "experimentName": "reg_exp",
                    "keepArtifacts": True,
                    "modelFileUrlPattern": "file://temp/mldb-256_reg.cls",
                    "algorithm": "glz_linear",
                    "configuration": {
                        "glz_linear": {
                            "type": "glz",
                            "normalize": True,
                            "link_function": 'linear',
                            "regularization": 'l1',
                            "regularization_factor": 0.001
                        },

                        # It should be the same error as the previous one, but
                        # the key is too similar and confuses the error
                        # detector (because it contains a dot) Should the
                        # Configure class stop accepting dotted key it would
                        # solve this.
                        "glz_linear.BAR": {
                            "FOO": "linear"
                        }
                    },
                    "kfold": 2,
                    "mode": "regression",
                    "outputAccuracyDataset": True
                }
            })

    def test_classifier_allows_underscored_key_param(self):
        msg = "No feature vectors were produced as all"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put("/v1/procedures/regression_cls", {
                "type": "classifier.experiment",
                "params": {
                    "inputData": "SELECT {*} AS features, label AS label FROM foo",
                    "experimentName": "reg_exp",
                    "keepArtifacts": True,
                    "modelFileUrlPattern": "file://temp/mldb-256_reg.cls",
                    "algorithm": "glz_linear",
                    "configuration": {
                        "glz_linear": {
                            "type": "glz",
                            "normalize": True,
                            "link_function": 'linear',
                            "regularization": 'l1',
                            "regularization_factor": 0.001,
                            "_ignored" : "this key is ignored"
                        },

                        # The key is clearly different, no problem
                        "FOO": {
                            "FOO": "linear"
                        }
                    },
                    "kfold": 2,
                    "mode": "regression",
                    "outputAccuracyDataset": True
                }
            })


if __name__ == '__main__':
    mldb.run_tests()
