# -*- coding: utf-8 -*-
#
# MLDB-558-python-unicode.py
# mldb.ai inc, 2015
# Mich, 2016-02-08
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
from urllib import quote

mldb = mldb_wrapper.wrap(mldb) # noqa

class Utf8IdsTest(MldbUnitTest): # noqa

    def test_mldb_create_dataset(self):
        _id = u'épluche'
        mldb.create_dataset({
            'id' : _id,
            'type' : 'sparse.mutable'
        }).commit()

        # fetch escaped ascii
        url = quote(('/v1/datasets/' + _id).encode('utf8'))
        mldb.log(url)
        res = mldb.get(url)
        obj = res.json()
        self.assertEqual(obj['id'], _id)

        # fetch unescaped utf-8 str
        url = '/v1/datasets/épluche'
        mldb.log(url)
        res = mldb.get(url)
        obj = res.json()
        self.assertEqual(obj['id'], _id)

        # fetch unescaped unicode utf-8
        url = u'/v1/datasets/épluche'
        mldb.log(url)
        res = mldb.get(url)
        obj = res.json()
        self.assertEqual(obj['id'], _id)

    def test_mixed_utf8_escape(self):
        # the parser assumes utf-8 is already escaped
        _id = u'éé'
        mldb.create_dataset({
            'id' : _id,
            'type' : 'sparse.mutable'
        }).commit()

        # fetch escaped ascii
        url = u'/v1/datasets/é' + quote('é')
        mldb.log(url)
        res = mldb.get(url)
        mldb.log(res)

    def test_mldb_post_dataset(self):
        _id = u'époque'
        res = mldb.post('/v1/datasets', {
            'id' : _id,
            'type' : 'sparse.mutable'
        })
        mldb.log(mldb.get('/v1/datasets'))
        url = quote(('/v1/datasets/' + _id).encode('utf8'))
        mldb.log(url)
        res = mldb.get(url).json()
        self.assertEqual(res['id'], _id)

    def test_mldb_put_dataset(self):
        _id = 'épopée'
        url = quote('/v1/datasets/' + _id)
        mldb.log(url)
        mldb.put(url, {
            'type' : 'sparse.mutable'
        })
        res = mldb.get(url).json()
        self.assertEqual(res['id'].encode('utf8'), _id)

    def test_name_with_slash(self):
        _id = "name/with/slash"
        url = '/v1/datasets/' + quote(_id, safe='')
        mldb.log(url)
        mldb.put(url, {
            'type' : 'sparse.mutable'
        })
        res = mldb.get(url).json()
        self.assertEqual(res['id'], _id)

    def test_name_with_space(self):
        _id = "name with space"
        url = '/v1/datasets/' + quote(_id)
        mldb.log(url)
        mldb.put(url, {
            'type' : 'sparse.mutable'
        })
        res = mldb.get(url).json()
        self.assertEqual(res['id'], _id)

    def execute_sequence(self, _id):
        url = '/v1/datasets/' + quote(_id, safe='')
        mldb.log(url)
        res = mldb.put(url, {
            'type' : 'sparse.mutable'
        })

        res = mldb.get(res.headers['Location']).json()
        self.assertEqual(res['id'].encode('utf8'), _id)

        res = mldb.get(url).json()
        self.assertEqual(res['id'].encode('utf8'), _id)

        mldb.delete(url)
        with self.assertMldbRaises(status_code=404):
            mldb.get(url)

        res = mldb.post('/v1/datasets', {
            'id' : _id,
            'type' : 'sparse.mutable'
        })

        res = mldb.get(res.headers['Location']).json()
        self.assertEqual(res['id'].encode('utf8'), _id)

        res = mldb.get(url).json()
        self.assertEqual(res['id'].encode('utf8'), _id)

        mldb.delete(url)
        with self.assertMldbRaises(status_code=404):
            mldb.get(url)

    def test_cedille(self):
        self.execute_sequence('françois')

    def test_cedille_and_slash(self):
        self.execute_sequence('françois/michel')

    def test_cedille_and_whitespace(self):
        self.execute_sequence('françois michel')

    def test_cedille_whitespace_slash_question_mark(self):
        self.execute_sequence('"françois says hello/goodbye, eh?"')

    def test_plus_sign(self):
        self.execute_sequence('"a+b"')

        mldb.post('/v1/datasets', {
            'id' : 'a+b',
            'type' : 'sparse.mutable'
        })
        mldb.get('/v1/datasets/a+b').json()

    def test_extra_5(self):
        mldb.put('/v1/datasets/ds', {
            'type' : 'sparse.mutable'
        })
        mldb.post('/v1/datasets/ds/commit')

        mldb.put('/v1/procedures/' + quote('françois'), {
            'type' : 'transform',
            'params' : {
                'inputData' : 'SELECT * FROM ds',
                'outputDataset' : {
                    'id' : 'outDs',
                    'type' : 'sparse.mutable'
                }
            }
        })

        url = '/v1/procedures/' + quote('françois') + '/runs/' \
              + quote('michêl')
        mldb.put(url)
        mldb.get(url)

    def test_select_over_utf8_dataset_name(self):
        _id = "hellô"
        mldb.create_dataset({
            "id": _id,
            "type": "embedding"
        })
        result = mldb.get("/v1/query", q=u"select * from \"hellô\"")
        mldb.log(result.text)

        result = mldb.get("/v1/datasets")
        mldb.log(result.text)

        result = mldb.put("/v1/datasets/hôwdy", {"type": 'embedding'})
        mldb.log(result.text)

        result = mldb.get("/v1/datasets")
        mldb.log(result.text)

        result = mldb.post("/v1/datasets", {
            "id": "hî",
            "type": 'embedding'
        })
        mldb.log(result.text)

        result = mldb.get("/v1/datasets")
        mldb.log(result.text)

    def test_slash_dataset(self):
        ds = mldb.create_dataset({
            'id' : 's/lash',
            'type' : 'sparse.mutable'
        })
        ds.commit()
        mldb.log(mldb.get('/v1/query', q='SELECT * FROM "s/lash"'))

if __name__ == '__main__':
    mldb.run_tests()
