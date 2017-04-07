#
# MLDB-2181_null_feature_model_test.py
# Francois-Michel L'Heureux, 2017-04-07
# This file is part of MLDB. Copyright 2017 Element.ai. All rights reserved.
#
# Basically, when we train with a feature that is always NULL, the model will
# store it as a numeric type. Then, when we test, if the examples are strings,
# it fails.
#
# In the following test, the column "issue" represents the issue. We need to go
# through a transform because we can't push null values with mldb.record_row.
#

import random
import tempfile

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2181NullFeatureModelTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'pre_ds', 'type' : 'sparse.mutable'})
        for idx in xrange(10):
            ds.record_row('row{}'.format(idx), [
                ['line', idx, 0],
                ['label', 1, 0],
                ['feature', random.random(), 0],
                ['noise', random.random(), 0],
                ['a_or_b', 'a' if random.random() > 0.5 else 'b', 0]
            ])

        for idx in xrange(10, 20):
            ds.record_row('row{}'.format(idx), [
                ['line', idx, 0],
                ['label', 0, 0],
                ['feature', random.random() + 0.6, 0],
                ['noise', random.random(), 0],
                ['a_or_b', 'a' if random.random() > 0.5 else 'b', 0]
            ])
        ds.commit()

        mldb.post('/v1/procedures', {
            'type': 'transform',
            'params' : {
                'inputData': 'SELECT *, NULL as issue FROM pre_ds',
                'outputDataset': {
                    'id' : 'train_ds',
                    'type': 'sparse.mutable'
                }
            }
        })

        cls.model_file = tempfile.NamedTemporaryFile(dir='build/x86_64/tmp')

        mldb.post('/v1/procedures', {
            "type": "classifier.train",
            "params": {
                "mode": 'boolean',
                "trainingData":
                    'SELECT {feature, noise, issue} AS features, label '
                    'FROM train_ds',
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
                                "max_depth": 5,
                                "random_feature_propn": .1,
                            },
                            "min_iter": 5,
                            "max_iter": 30
                        },
                        "num_bags": 5
                    }
                },
                "functionName": 'score_it',
                'modelFileUrl': 'file://' + cls.model_file.name
            }
        })

    def test_string_over_null(self):
        ds = mldb.create_dataset({
            'id': 'test_string_over_null_ds',
            'type': 'sparse.mutable'
        })
        for idx in xrange(10):
            ds.record_row('row{}'.format(idx), [
                ['line', idx, 0],
                ['label', 0, 0],
                ['feature', random.random() + 0.6, 0],
                ['noise', random.random(), 0],
                ['issue', 'STRING', 0]
            ])
        ds.commit()

        mldb.post('/v1/procedures', {
            'type': 'classifier.test',
            'params' : {
                'mode': 'boolean',
                'testingData':
                    'SELECT score_it({features: {feature, noise, issue}})[score] AS score, ' \
                    'label FROM test_string_over_null_ds'
            }
        })

    def test_num_over_null(self):
        ds = mldb.create_dataset({
            'id': 'test_num_over_null_ds',
            'type': 'sparse.mutable'
        })
        for idx in xrange(10):
            ds.record_row('row{}'.format(idx), [
                ['line', idx, 0],
                ['label', 0, 0],
                ['feature', random.random() + 0.6, 0],
                ['noise', random.random(), 0],
                ['issue', random.random() * 10, 0]
            ])
        ds.commit()

        mldb.post('/v1/procedures', {
            'type': 'classifier.test',
            'params' : {
                'mode': 'boolean',
                'testingData':
                    'SELECT score_it({features: {feature, noise, issue}})[score] AS score, ' \
                    'label FROM test_num_over_null_ds'
            }
        })

    def test_c_over_a_or_b(self):
        ds = mldb.create_dataset({
            'id': 'test_c_over_a_or_b_ds',
            'type': 'sparse.mutable'
        })
        for idx in xrange(10):
            ds.record_row('row{}'.format(idx), [
                ['line', idx, 0],
                ['label', 0, 0],
                ['feature', random.random() + 0.6, 0],
                ['noise', random.random(), 0],
                ['a_or_b', 'c', 0]
            ])
        ds.commit()

        mldb.post('/v1/procedures', {
            'type': 'classifier.test',
            'params' : {
                'mode': 'boolean',
                'testingData':
                    'SELECT score_it({features: {feature, noise, a_or_b}})[score] AS score, ' \
                    'label FROM test_c_over_a_or_b_ds'
            }
        })

if __name__ == '__main__':
    mldb.run_tests()
