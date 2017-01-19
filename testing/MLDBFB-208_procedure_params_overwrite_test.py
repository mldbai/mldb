#
# MLDBFB-208_procedure_params_overwrite_test.py
# Mich, 2016-02-02
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class GenericProcedureTest(MldbUnitTest): # noqa

    def test_it(self):
        ds = mldb.create_dataset({
            'id' : 'ds',
            'type' : 'sparse.mutable'
        })
        ds.record_row('row1', [['colA', 1, 1]])
        ds.commit()

        mldb.put('/v1/procedures/transform', {
            'type' : 'transform',
            'params' : {
                'inputData' : 'SELECT * FROM foo',
                'outputDataset' : {
                    'id' : 'bar',
                    'type' : 'sparse.mutable'
                },
                'runOnCreation' : 0
            }
        })

        # FIXME Currently fails, it uses foo as input rather than ds (aka the
        # overriden parameters are not taken into account)
        resp = mldb.post('/v1/procedures/transform/runs', {
            'params' : {
                'inputData' : 'SELECT * FROM ds',
                'outputDataset' : {
                    'id' : 'out',
                    'type' : 'sparse.mutable'
                }
            }
        })

        # outputDataset was overriden in the run to output to out
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                     "dataset entry 'bar' doesn't exist"):
            mldb.query("SELECT * FROM bar")

        # out dataset was created and contains the expected result
        res = mldb.query("SELECT * FROM out")
        self.assertTableResultEquals(res, [['_rowName', 'colA'], ['row1', 1]])

if __name__ == '__main__':
    mldb.run_tests()
