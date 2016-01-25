#
# python_mldb_interface_test.py
# Mich, 2016-01-25
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#
import unittest

if False:
    mldb_wrapper = None

mldb = mldb_wrapper.wrap(mldb) # noqa


class PythonMldbInterfaceTest(unittest.TestCase):

    def test_log(self):
        mldb.log("Testing log")

    def test_get(self):
        res = mldb.get('/ping')
        mldb.log(res)
        mldb.log(res.text)
        mldb.log(type(res))
        mldb.log(dir(res))

        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.get("/unexisting")

        with self.assertRaises(mldb_wrapper.Exception):
            mldb.get("/unexisting", "this is expected to be a dict")

        with self.assertRaises(mldb_wrapper.Exception):
            # expects a single *args
            mldb.get("/unexisting", {"a" : "b"}, {"c" : "d"})

        with self.assertRaises(mldb_wrapper.Exception):
            # shouldn't define args + xargs
            mldb.get("/unexisting", {"a" : "b"}, a='coco')

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
        mldb.post(url + '/rows', rowName='row2', columns=[['colB', 1, 0]])
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

        res2 = mldb.get('/v1/query', {'q' : 'SELECT * FROM ds'}).json()
        self.assertEqual(res2, res)

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

        res = mldb.query('SELECT * FROM ds').json()
        self.assertEqual(res, [['_rowName', u'colA'], ['row1', 1]])

if __name__ == '__main__':
    mldb.run_tests()
