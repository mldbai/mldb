#
# test_classifier_test_proc.py
# Simon Lemieux, 2016-08-18
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
# This tests the `classifier.test` procedure.
#
from __future__ import division
from pprint import pformat
import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa




class TestClassifierTestProc(MldbUnitTest):  # noqa

    maxDiff = None

    def assert_recursive_almost_equal(self, a, b, places=7):
        for key, item_a in a.iteritems():
            item_b = b[key]
            if isinstance(item_a, dict):
                self.assertTrue(isinstance(item_b, dict))
                self.assertEqual(len(item_a), len(item_b))
                self.assert_recursive_almost_equal(item_a, item_b, places)
            else:
                self.assertAlmostEqual(item_a, item_b, places)

    @classmethod
    def setUpClass(cls):
        # input dataset for classifier.test
        headers = ['score', 'bool_label', 'cat_label', 'reg_label', 'weight']
        data = """
            1 0 1 10 1
            1 0 2 10 3
            2 1 3 20 3
            3 1 1 40 1
            """
        data = [map(float, row.strip().split())
                for row in data.strip().split('\n')]

        mldb.put('/v1/datasets/ds', {
            'type': 'tabular'
        })
        for i, row in enumerate(data):
            mldb.log(row)
            mldb.post('/v1/datasets/ds/rows', {
                'rowName': str(i),
                'columns': [[col,val,0] for col,val in zip(headers, row)]
            })
        mldb.post('/v1/datasets/ds/commit')

        mldb.log(mldb.query("""SELECT * FROM ds"""))

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
        # TODO test the output of res

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
        mldb.log(res)
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
        mldb.log(res)
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


if __name__ == '__main__':
    mldb.run_tests()
