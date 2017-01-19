#
# MLDB-1724-naive-bayes.py
# Simon Lemieux, 15 juin 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import csv
from StringIO import StringIO

mldb = mldb_wrapper.wrap(mldb) # noqa


def try_parse_to_float(x):
    try:
        return float(x)
    except:
        return x


class Mldb1724(MldbUnitTest):
    @classmethod
    def setUpClass(self):
        # example from wikipedia
        # https://en.wikipedia.org/wiki/Naive_Bayes_classifier
        data = StringIO(
"""gender height weight foot_size
male 6 180 12
male 5.92 190 11
male 5.58 170 12
male 5.92 165 10
female 5 100 6
female 5.5 150 8
female 5.42 130 7
female 5.75 150 9""")
        reader = csv.DictReader(data, delimiter=' ')

        ds = mldb.create_dataset({"id": "data",
                                  "type": "sparse.mutable"})
        for i, row in enumerate(reader):
            ds.record_row(
                'dude_' + str(i),
                [[col, try_parse_to_float(val), 0] for col,val
                 in row.iteritems()])
        ds.commit()

    def _do_test(self, mode):
        mldb.post('/v1/procedures', {
            'type': 'classifier.train',
            'params': {
                'trainingData': """
                    SELECT {* EXCLUDING (gender)} AS features,
                           gender = 'male' as label
                    FROM data
                    """,
                'mode': mode,
                'algorithm': 'naive_bayes',
                'configuration': {
                    'naive_bayes': {
                        'type': 'naive_bayes',
                        'verbosity': 3
                    },
                    "dt": {
                        "_note": "Plain decision tree",
                        "type": "decision_tree",
                        "max_depth": 8,
                        "verbosity": 3,
                        "update_alg": "prob"
                    }
                },
                'modelFileUrl': 'file://tmp/model.cls.gz',
                'functionName': 'classify',
                'runOnCreation': True
            }
        })

        for feats, target in [
            ('foot_size:11', 1),
            ('height:5.8, weight:200', 1),
            ('height:6, weight:180, foot_size:12', 1),
            ('height:5, weight:120, foot_size:6', 0),
            ('height:5.4, foot_size:6', 0),
                ]:
            prediction = mldb.get('/v1/query',
                q="""SELECT classify({features: {%s}}) as *""" % feats,
                format='aos').json();
            mldb.log(prediction)
            if 'scores.1' in prediction[0]:
                score = prediction[0]['scores.1']
            else:
                score = prediction[0]['score']
            print score
            self.assertLess(score - target, .01)


    def test_naive_bayes(self):
        self._do_test("boolean")

    def test_categorical(self):
        self._do_test("categorical")

mldb.run_tests()
