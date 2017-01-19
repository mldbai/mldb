#
# python_mldb_interface_test.py
# Mich, 2016-01-25
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest

if False:
    mldb_wrapper = None

mldb = mldb_wrapper.wrap(mldb) # noqa


class PythonMldbInterfaceTest(MldbUnitTest): # noqa

    def test_log(self):
        mldb.log("Testing log")

    def test_get(self):
        mldb.get('/ping')
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.get("/unexisting")

    def test_put(self):
        mldb.put("/v1/datasets/test_put", {
            'type' : 'sparse.mutable'
        })

    def test_post(self):
        res = mldb.post("/v1/datasets", {
            'type' : 'sparse.mutable'
        })
        id_ = res.json()['id']
        url = '/v1/datasets/{}'.format(id_)

        mldb.post(url + '/rows', {
            'rowName' : 'row1',
            'columns' : [['colA', 1, 0]]
        })
        mldb.post(url + '/commit')

    def test_delete(self):
        # this test depends on put and post
        url = '/v1/datasets/ds'
        mldb.put(url, {
            'type' : 'sparse.mutable'
        })
        mldb.post(url + '/commit')
        mldb.delete(url)

    def test_advanced_get_parameters(self):
        url = '/v1/datasets/ds'
        mldb.put(url, {
            'type' : 'sparse.mutable'
        })
        mldb.post(url + '/rows', {
            'rowName' : 'row1',
            'columns' : [['colA', 1, 0]]
        })
        mldb.post(url + '/commit')

        res = mldb.get(url).json()
        self.assertEqual(res['status']['columnCount'], 1)
        self.assertEqual(res['status']['rowCount'], 1)

        res = mldb.get('/v1/query', q='SELECT * FROM ds').json()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['rowName'], 'row1')
        self.assertEqual(res[0]['columns'],
                         [['colA', 1, '1970-01-01T00:00:00Z']])

    def test_query(self):
        url = '/v1/datasets/ds'
        mldb.put(url, {
            'type' : 'sparse.mutable'
        })
        mldb.post(url + '/rows', {
            'rowName' : 'row1',
            'columns' : [['colA', 1, 0]]
        })
        mldb.post(url + '/commit')

        res = mldb.query('SELECT * FROM ds')
        self.assertEqual(res, [['_rowName', u'colA'], ['row1', 1]])

    def test_response_exception(self):
        url = "/unexisting"
        try:
            mldb.get(url)
        except mldb_wrapper.ResponseException as response_exception:
            pass

        res = response_exception.response
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.url, url)

    def test_get_bound_http_address(self):
        mldb.log(mldb.get_http_bound_address())

    def test_assert_table_result_equals(self):
        url = '/v1/datasets/ds'
        mldb.put(url, {
            'type' : 'sparse.mutable'
        })
        mldb.post(url + '/rows', {
            'rowName' : 'row1',
            'columns' : [['colA', 1, 0]]
        })
        mldb.post(url + '/rows', {
            'rowName' : 'row2',
            'columns' : [['colB', 2, 1]]
        })
        mldb.post(url + '/commit')

        res = mldb.query("SELECT colA, colB FROM ds ORDER BY rowName()")
        self.assertTableResultEquals(res, [
            ['_rowName', 'colA', 'colB'],
            ['row1', 1, None],
            ['row2', None, 2]
         ])

    def test_assert_full_result_equals(self):
        url = '/v1/datasets/ds'
        mldb.put(url, {
            'type' : 'sparse.mutable'
        })
        mldb.post(url + '/rows', {
            'rowName' : 'row1',
            'columns' : [['colA', 1, 0]]
        })
        mldb.post(url + '/rows', {
            'rowName' : 'row2',
            'columns' : [['colB', 2, 1]]
        })
        mldb.post(url + '/commit')

        res = mldb.get('/v1/query',
                       q="SELECT colA, colB FROM ds ORDER BY rowName()").json()
        self.assertFullResultEquals(res, [
            {
                'rowName' : 'row1',
                'columns' : [['colA', 1, '1970-01-01T00:00:00Z'],
                             ['colB', None, '-Inf']]
            },
            {
                'rowName' : 'row2',
                'columns' : [
                    ['colA', None, '-Inf'],
                    ['colB', 2, '1970-01-01T00:00:01Z']]
            }
        ])

    def test_assert_mldb_raises(self):
        with self.assertRaises(AssertionError):
            with self.assertMldbRaises():
                mldb.get("/v1/datasets")

        with self.assertMldbRaises():
            mldb.get("/warp")

        with self.assertMldbRaises(status_code=404) as exc:
            mldb.get("/warp")
        self.assertEqual(exc.exception.response.status_code, 404)

        with self.assertRaises(AssertionError):
            with self.assertMldbRaises(status_code=403):
                mldb.get("/warp")

        with self.assertRaises(AssertionError):
            with self.assertMldbRaises(expected_regexp="rantanplan"):
                mldb.get("/warp")

        with self.assertMldbRaises(
                expected_regexp="unknown resource GET /warp"):
            mldb.get("/warp")

        with self.assertMldbRaises(expected_regexp="resource GET"):
            mldb.get("/warp")

if __name__ == '__main__':
    mldb.run_tests()
