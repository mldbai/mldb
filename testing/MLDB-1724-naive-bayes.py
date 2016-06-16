#
# MLDB-1724-naive-bayes.py
# Francois Maillet, 15 juin 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
import csv
from StringIO import StringIO

mldb = mldb_wrapper.wrap(mldb) # noqa


class Mldb1721(MldbUnitTest):
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
female 5.75 150 9""".strip())
        reader = csv.DictReader(data, delimiter=' ')

        ds = mldb.create_dataset({"id": "data",
                                  "type": "sparse.mutable"})
        for i, row in enumerate(reader):
            ds.record_row(
                'dude_' + str(i),
                [[col, val, 0] for col,val in row.iteritems()])
        ds.commit()

        mldb.log(mldb.query('select * from data'))

    def test_naive_bayes(self):
        mldb.post('/v1/procedures', {
            'type': 'classifier.train',
            'params': {
                'trainingData': """
                    SELECT {* EXCLUDING (gender)} AS features,
                           gender = 'male' as label
                    FROM data
                    """,
                'mode': 'boolean',
                'algorithm': 'naive_bayes',
                'configuration': {
                    'naive_bayes': {
                        'type': 'naive_bayes'
                    }
                },
                'modelFileUrl': 'file://model.cls.gz',
                'functionName': 'classify',
                'runOnCreation': True
            }
        })

        # TODO
        # self.assertEqual(mldb.query("""
        #     SELECT classify({
        #         height:6.02
        #     }
        # """), 1)

mldb.run_tests()
