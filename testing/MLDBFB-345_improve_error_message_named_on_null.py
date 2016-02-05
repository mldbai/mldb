#
# MLDBFB-345_improve_error_message_named_on_null.py
# Mich, 2016-02-01
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

class ImproveErrorMessageNamedOnNullTest(MldbUnitTest): # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({
            'id' : 'ds',
            'type' : 'sparse.mutable'
        })

        ds.record_row('row1', [['behA', 'a', 0]])
        ds.record_row('row2', [['behB', 'b', 0]])
        ds.commit()

    def test_working_case(self):
        mldb.post('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' :
                    # works because we only work on the row where behA != null
                    'SELECT * NAMED behA FROM ds WHERE behA IS NOT NULL',
                'outputDataset' : {
                    'id': 'out1'
                },
                'runOnCreation' : True
            }
        })

    def test_non_working_case(self):
        with self.assertRaises(mldb_wrapper.ResponseException) as exc: # noqa
            mldb.post('/v1/procedures', {
                'type' : 'transform',
                'params' : {
                    'inputData' : 'SELECT * NAMED behA FROM ds',
                    'outputDataset' : {
                        'id': 'out2'
                    },
                    'runOnCreation' : True
                }
            })
        # FIXME: It's ok to fail, but the error message should be helpful
        # currently returns "Can't convert from empty to string"
        mldb.log(exc.exception.response.json())

if __name__ == '__main__':
    mldb.run_tests()
