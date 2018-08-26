#
# MLDB-2114_plugin_post_no_data_404_test.py
# Francois-Michel L'Heureux, 2017-01-20
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2114PluginPostNoData404Test(MldbUnitTest):  # noqa

    def test_empty_json(self):
        """
        Empty JSON returns the proper code
        """
        mldb.put("/v1/plugins/mldb2114", {
            "type": "python",
            "params": {
                "source": {
                    "routes": """
if request.verb in ['GET', 'DELETE']:
    request.set_return({}, 200)
else:
    request.set_return({}, 201)
"""

                }
            }
        })

        res = mldb.get('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), {})

        res = mldb.post('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(res.status_code, 201)
        self.assertEqual(res.json(), {})

        res = mldb.put('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(res.status_code, 201)
        self.assertEqual(res.json(), {})

        res = mldb.delete('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), {})

    def test_null_json(self):
        mldb.put("/v1/plugins/mldb2114", {
            "type": "python",
            "params": {
                "source": {
                    "routes": """request.set_return(None, 200)"""

                }
            }
        })

        res = mldb.get('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), None)

        res = mldb.post('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), None)

        res = mldb.put('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), None)

        res = mldb.delete('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), None)

    def test_empty_str_json(self):
        mldb.put("/v1/plugins/mldb2114", {
            "type": "python",
            "params": {
                "source": {
                    "routes": """request.set_return("", 200)"""

                }
            }
        })

        res = mldb.get('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), "")

        res = mldb.post('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), "")

        res = mldb.put('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), "")

        res = mldb.delete('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), "")

    def test_no_set_return(self):
        mldb.put("/v1/plugins/mldb2114", {
            "type": "python",
            "params": {
                "source": {
                    "routes": """mldb.log('no return')"""

                }
            }
        })

        msg = "Return value is required for route handlers but not set"

        with self.assertRaisesRegex(mldb_wrapper.ResponseException, msg) as e:
            mldb.get('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(e.exception.response.status_code, 500)

        with self.assertRaisesRegex(mldb_wrapper.ResponseException, msg) as e:
            mldb.post('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(e.exception.response.status_code, 500)

        with self.assertRaisesRegex(mldb_wrapper.ResponseException, msg) as e:
            mldb.put('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(e.exception.response.status_code, 500)

        with self.assertRaisesRegex(mldb_wrapper.ResponseException, msg) as e:
            mldb.delete('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(e.exception.response.status_code, 500)

    def test_set_return_0(self):
        mldb.put("/v1/plugins/mldb2114", {
            "type": "python",
            "params": {
                "source": {
                    "routes": """request.set_return("", 0)"""

                }
            }
        })

        with self.assertRaises(mldb_wrapper.ResponseException) as e:
            mldb.get('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(e.exception.response.status_code, 500)

        with self.assertRaises(mldb_wrapper.ResponseException) as e:
            mldb.post('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(e.exception.response.status_code, 500)

        with self.assertRaises(mldb_wrapper.ResponseException) as e:
            mldb.put('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(e.exception.response.status_code, 500)

        with self.assertRaises(mldb_wrapper.ResponseException) as e:
            mldb.delete('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(e.exception.response.status_code, 500)

if __name__ == '__main__':
    request.set_return(mldb.run_tests())
