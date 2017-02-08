#
# MLDB-1104-input-data-spec.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#
import unittest

import datetime
import random

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


class InputDataSpecTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.load_kmeans_dataset()
        cls.load_classifier_dataset()

    @classmethod
    def load_kmeans_dataset(cls):
        kmeans_example = mldb.create_dataset({
            "type": "sparse.mutable",
            'id' : 'kmeans_example'
        })
        now = datetime.datetime.now()
        for i in xrange(100):
            val_x = float(random.randint(-5, 5))
            val_y = float(random.randint(-5, 5))
            row = [['x', val_x, now], ['y', val_y, now]]
            kmeans_example.record_row('row_%d' % i, row)
        kmeans_example.commit()

    def train_kmeans(self, training_data):
        metric = "euclidean"
        mldb.put("/v1/procedures/kmeans", {
            'type' : 'kmeans.train',
            'params' : {
                'trainingData' : training_data,
                'centroidsDataset' : {
                    'id' : 'kmeans_centroids',
                    'type' : 'embedding',
                    'params': {
                        'metric': metric
                    }
                },
                'numClusters' : 2,
                'metric': metric
            }
        })

    def train_svd(self, training_data):
        mldb.put("/v1/procedures/svd", {
            'type' : 'svd.train',
            'params' : {
                'trainingData' : training_data,
                'runOnCreation' : True
            }
        })

    @classmethod
    def load_classifier_dataset(cls):
        dataset = mldb.create_dataset({
            "type": "sparse.mutable",
            "id": "iris_dataset"
        })

        with open("./mldb/testing/dataset/iris.data") as f:
            for i, line in enumerate(f):
                cols = []
                line_split = line.split(',')
                if len(line_split) != 5:
                    continue
                # Jemery's what if a feature is named label
                cols.append(["label", float(line_split[0]), 0]) # sepal length
                cols.append(["labels", float(line_split[1]), 0]) # sepal width
                cols.append(["petal length", float(line_split[2]), 0])
                cols.append(["petal width", float(line_split[3]), 0])
                cols.append(["features", line_split[4].strip('\n"'), 0]) #class
                dataset.record_row(str(i+1), cols)

        dataset.commit()

    def train_classifier(self, training_data):
        result = mldb.put("/v1/procedures/classifier", {
            'type' : 'classifier.train',
            'params' : {
                'trainingData' : training_data,
                "configuration": {
                    "type": "decision_tree",
                    "max_depth": 8,
                    "verbosity": 3,
                    "update_alg": "prob"
                },
                "modelFileUrl": "file://tmp/MLDB-1104.cls",
                "mode": "categorical",
                "functionName": "classifier_apply",
                'runOnCreation' : True
            }
        })
        return result.json()

    def test_train_kmeans(self):
        # KMEANS TRAIN PROCEDURE WITH BOTH TYPE OF INPUT DATA
        self.train_kmeans('select * from kmeans_example')
        self.train_kmeans('select x + y as x, y + x as y from kmeans_example')
        self.train_kmeans({'select' : '*', 'from' : {'id' : 'kmeans_example'}})

        # TEST ERROR CASE
        with self.assertRaises(mldb_wrapper.ResponseException):
            self.train_kmeans(
                'select x, y from kmeans_example group by x')
        with self.assertRaises(mldb_wrapper.ResponseException):
            self.train_kmeans(
                'select x, y from kmeans_example group by x having y > 2')

    def test_train_svd(self):
        self.train_svd('select * from kmeans_example')
        self.train_svd('select x, y from kmeans_example')
        self.train_svd('select x AS z, y from kmeans_example')
        self.train_svd('select * EXCLUDING(x) from kmeans_example')
        self.train_svd({'select' : '*', 'from' : {'id' : 'kmeans_example'}})
        self.train_svd('select x + 1, y from kmeans_example')

        with self.assertRaises(mldb_wrapper.ResponseException):
            self.train_svd('select x, y from kmeans_example group by x')
        with self.assertRaises(mldb_wrapper.ResponseException):
            self.train_svd(
                'select x, y from kmeans_example group by x having y > 2')

    def test_train_classifier(self):
        mldb.log(self.train_classifier(
            "select {label, labels} as features, features as label "
            "from iris_dataset"))

        result = mldb.get(
            "/v1/query",
            q="SELECT classifier_apply({{label, labels} as features}) as *, features from iris_dataset")
        rows = result.json()
        mldb.log("-------------------------------");
        mldb.log(rows)

        # compare the classifier results on the train data with the original
        # label
        count = 0
        for row in rows:
            _max = 0
            category = ""
            for column in row['columns'][1:4]:
                if column[1] > _max:
                    _max = column[1]
                    # remove the leading scores. and quotation marks
                    category = column[0][10:-3]
            if category != row['columns'][0][1]:
                count += 1

        # misclassified result should be a small fraction
        self.assertTrue(
            float(count) / len(rows) < 0.2,
            'the classifier results on the train data are strangely low')

if __name__ == '__main__':
    mldb.run_tests()
