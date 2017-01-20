#
# MLDB-1893_get_params_mixin.py
# Francois-Michel L Heureux, 2016-08-09
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1893GetParamsMixin(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['colA', 1, 1]])
        ds.commit()

    def test_query_string(self):
        res = mldb.get('/v1/query', q='SELECT * FROM ds', format='table')
        self.assertTableResultEquals(res.json(), [
            ['_rowName', 'colA'],
            ['row1', 1]
        ])

    def test_body(self):
        res = mldb.get('/v1/query', data={
            'q' : 'SELECT * FROM ds',
            'format' : 'table'
        })
        self.assertTableResultEquals(res.json(), [
            ['_rowName', 'colA'],
            ['row1', 1]
        ])

    def test_mixing(self):
        msg = 'You cannot mix query string and body parameters'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.get('/v1/query', q='SELECT * FROM ds', data={
                'format' : 'table'
            })

if __name__ == '__main__':
    mldb.run_tests()
