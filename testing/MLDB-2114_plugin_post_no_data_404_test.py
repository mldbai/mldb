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
if mldb.plugin.rest_params.verb in ['GET', 'DELETE']:
    mldb.plugin.set_return({}, 200)
else:
    mldb.plugin.set_return({}, 201)
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
                    "routes": """mldb.plugin.set_return(None, 200)"""

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
                    "routes": """mldb.plugin.set_return("", 200)"""

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

        msg = "The route did not set a return code"

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg) as e:
            mldb.get('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(e.exception.response.status_code, 500)

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg) as e:
            mldb.post('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(e.exception.response.status_code, 500)

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg) as e:
            mldb.put('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(e.exception.response.status_code, 500)

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg) as e:
            mldb.delete('/v1/plugins/mldb2114/routes/foo')
        self.assertEqual(e.exception.response.status_code, 500)

    def test_set_return_0(self):
        mldb.put("/v1/plugins/mldb2114", {
            "type": "python",
            "params": {
                "source": {
                    "routes": """mldb.plugin.set_return("", 0)"""

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
    mldb.run_tests()
