#
# MLDBFB-332-transform_input_sum_doesnt_exist_test.py
# Mich, 2016-02-01
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa


class SumDoesNotExistTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        ds = mldb.create_dataset({
            'id' : 'ds',
            'type' : 'sparse.mutable'
        })
        ds.record_row('row1', [['colA', 1, 1]])
        ds.commit()

    @unittest.expectedFailure
    def test_object(self):
        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : {
                    'select' : 'sum({*})',
                    'from' : 'ds'
                },
                'outputDataset' : {
                    'id' : 'res',
                    'type' : 'sparse.mutable'
                },
                'runOnCreation' : True
            }
        })

    def test_object_w_group_by(self):
        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : {
                    'select' : 'sum({*})',
                    'from' : 'ds',
                    'groupBy' : '1'
                },
                'outputDataset' : {
                    'id' : 'res',
                    'type' : 'sparse.mutable'
                },
                'runOnCreation' : True
            }
        })

    @unittest.expectedFailure
    def test_object_w_named(self):
        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : {
                    'select' : 'sum({*})',
                    'from' : 'ds',
                    'named' : 'coco'
                },
                'outputDataset' : {
                    'id' : 'res',
                    'type' : 'sparse.mutable'
                },
                'runOnCreation' : True
            }
        })

    @unittest.expectedFailure
    def test_object_w_group_by_and_named(self):
        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : {
                    'select' : 'sum({*})',
                    'from' : 'ds',
                    'groupBy' : '1',
                    'named' : 'coco'
                },
                'outputDataset' : {
                    'id' : 'res',
                    'type' : 'sparse.mutable'
                },
                'runOnCreation' : True
            }
        })

    def test_plain_sql(self):
        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : 'SELECT sum({*}) FROM ds',
                'outputDataset' : {
                    'id' : 'res',
                    'type' : 'sparse.mutable'
                },
            }
        })

    def test_plain_sql_w_group_by(self):
        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : 'SELECT sum({*}) FROM ds GROUP BY 1',
                'outputDataset' : {
                    'id' : 'res',
                    'type' : 'sparse.mutable'
                },
            }
        })


if __name__ == '__main__':
    mldb.run_tests()
