#
# MLDBFB-332-transform_input_sum_doesnt_exist_test.py
# Mich, 2016-02-01
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class SumDoesNotExistTest(unittest.TestCase):

    def test_it(self):
        ds = mldb.create_dataset({
            'id' : 'ds',
            'type' : 'sparse.mutable'
        })
        ds.record_row('row1', [['colA', 1, 1]])
        ds.commit()

        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : {
                    'select' : 'sum({*})',
                    'from' : 'ds',
                    'named' : 'res'
                },
                'outputDataset' : {
                    'id' : 'res',
                    'type' : 'sparse.mutable'
                },
                'runOnCreation' : True
            }
        })

if __name__ == '__main__':
    mldb.run_tests()
