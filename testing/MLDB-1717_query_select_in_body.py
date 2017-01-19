#
# MLDB-1717_query_select_in_body.py
# Mich, 2016-06-10
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1717QuerySelectInBodyTest(MldbUnitTest):

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['a', 1, 1]])
        ds.commit()

    def test_as_query_string(self):
        res = mldb.get('/v1/query', q="SELECT * FROM ds")
        self.assertFullResultEquals(res.json(), [{
            'rowName' : 'row1',
            'columns' : [['a', 1, "1970-01-01T00:00:01Z"]]
        }])

    def test_as_body(self):
        res = mldb.get('/v1/query', data={'q' : "SELECT * FROM ds"})
        self.assertFullResultEquals(res.json(), [{
            'rowName' : 'row1',
            'columns' : [['a', 1, "1970-01-01T00:00:01Z"]]
        }])

    def test_query_function(self):
        # Query function now uses body instead of query string
        res = mldb.query("SELECT * FROM ds")
        self.assertTableResultEquals(res, [
            ['_rowName', 'a'],
            ['row1', 1]
        ])

if __name__ == '__main__':
    mldb.run_tests()
