#
# mongodb_plugin_test.py
# Mich, 2016-08-02
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
import unittest
import json

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MongodbPluginTest(MldbUnitTest):  # noqa

    def test_import_available(self):
        res = mldb.get('/v1/types/procedures')
        self.assertTrue('mongodb.import' in res.json())

    def test_record_available(self):
        res = mldb.get('/v1/types/datasets')
        self.assertTrue('mongodb.record' in res.json())

    def test_query_available(self):
        res = mldb.get('/v1/types/functions')
        self.assertTrue('mongodb.query' in res.json())

    def test_dataset_available(self):
        res = mldb.get('/v1/types/datasets')
        self.assertTrue('mongodb.dataset' in res.json())

    @unittest.skip("Requires local mldb + data to import")
    def test_import(self):
        # Example of import procedure. select, named, where, limit and offset
        # are supported options.
        res = mldb.post('/v1/procedures', {
            'type' : 'mongodb.import',
            'params' : {
                'connectionScheme' : 'mongodb://localhost:27017/tutorial',
                'collection' : 'users',
                'outputDataset' : {
                    'id' : 'out',
                    'type' : 'sparse.mutable'
                }
            }
        })

        mldb.log(res)
        mldb.log(mldb.query("SELECT * FROM out LIMIT 4"))

    def test_invalid_connection_scheme(self):
        msg = 'the minimal connectionScheme format is'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post('/v1/procedures', {
                'type' : 'mongodb.import',
                'params' : {
                    'connectionScheme' : 'mongodb://',
                    'collection' : 'users',
                    'outputDataset' : {
                        'id' : 'out',
                        'type' : 'sparse.mutable'
                    }
                }
            })

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post('/v1/procedures', {
                'type' : 'mongodb.import',
                'params' : {
                    'connectionScheme' : 'bouette://',
                    'collection' : 'users',
                    'outputDataset' : {
                        'id' : 'out',
                        'type' : 'sparse.mutable'
                    }
                }
            })

        msg = 'connectionScheme is a required property'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post('/v1/procedures', {
                'type' : 'mongodb.import',
                'params' : {
                    'connectionScheme' : '',
                    'collection' : 'users',
                    'outputDataset' : {
                        'id' : 'out',
                        'type' : 'sparse.mutable'
                    }
                }
            })

    def test_import_missing_param(self):
        msg = 'connectionScheme is a required property'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post('/v1/procedures', {
                'type' : 'mongodb.import',
                'params' : {
                    'collection' : 'users',
                    'outputDataset' : {
                        'id' : 'out',
                        'type' : 'sparse.mutable'
                    }
                }
            })

        msg = 'collection is a required property and must not be empty'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post('/v1/procedures', {
                'type' : 'mongodb.import',
                'params' : {
                    'connectionScheme' : 'mongodb://localhost:27017/tutorial',
                    'outputDataset' : {
                        'id' : 'out',
                        'type' : 'sparse.mutable'
                    }
                }
            })

    @unittest.skip("Requires local mldb")
    def test_record(self):
        # Example of dataset mongodb.record.
        # Pymongo should be used to assert the test wrote properly.
        res = mldb.create_dataset({
            'id' : 'ds_record',
            'type' : 'mongodb.record',
            'params' : {
                'connectionScheme' : 'mongodb://localhost:27017/tutorial',
                'collection' : 'newb',
            }
        })
        res.record_row('monNom', [['colA', 'valeur sure', 34]])
        res.commit()

    def test_record_missing_params(self):
        msg = 'connectionScheme is a required property'
        with self.assertRaisesRegexp(RuntimeError, msg):
            mldb.create_dataset({
                'id' : 'ds_err3',
                'type' : 'mongodb.record',
                'params' : {
                    'collection' : 'newb'
                }
            })

        msg = 'collection is a required property and must not be empty'
        with self.assertRaisesRegexp(RuntimeError, msg):
            mldb.create_dataset({
                'id' : 'ds_err4',
                'type' : 'mongodb.record',
                'params' : {
                    'connectionScheme' : 'mongodb://localhost:27017/tutorial'
                }
            })

    @unittest.skip("Requires local mldb + data to query")
    def test_query_first_row(self):
        # Example of a query passed straight to mongodb. The result comes back
        # formatted as an MLDB result.
        res = mldb.put('/v1/functions/mongo_query', {
            'type' : 'mongodb.query',
            'params' : {
                'connectionScheme' : 'mongodb://localhost:27017/tutorial',
                'collection' : 'users'
            }
        })
        mldb.log(res)
        query = json.dumps({
            'username' : {
                '$ne' : 'Finch'
            }
        })
        res = mldb.get('/v1/functions/mongo_query/application',
                       input={'query' : query})

    def test_query_first_row_missing_param(self):
        msg = 'connectionScheme is a required property'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/functions/mongo_query_err1', {
                'type' : 'mongodb.query',
                'params' : {
                    'collection' : 'users'
                }
            })

        msg = 'collection is a required property and must not be empty'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/functions/mongo_query_err2', {
                'type' : 'mongodb.query',
                'params' : {
                    'connectionScheme' : 'mongodb://localhost:27017/tutorial'
                }
            })

    @unittest.skip("Requires local mldb + data to query")
    def test_query_named_row(self):
        # Example of a query passed straight to mongodb. The result comes back
        # formatted as an MLDB result.
        res = mldb.put('/v1/functions/mongo_query', {
            'type' : 'mongodb.query',
            'params' : {
                'connectionScheme' : 'mongodb://localhost:27017/tutorial',
                'collection' : 'users',
                'output' : 'NAMED_COLUMNS'
            }
        }),
        mldb.log(res)
        query = json.dumps({
            'username' : {
                '$ne' : 'Finch'
            }
        })
        res = mldb.get('/v1/functions/mongo_query/application',
                       input={'query' : query})


    @unittest.skip("Unimplemented")
    def test_dataset(self):
        # Example of a read only mongo db dataset. MLDB queries can be made
        # over it.
        res = mldb.put('/v1/datasets/ds', {
            'type' : 'mongodb.dataset',
            'params' : {
                'connectionScheme' : 'mongodb://localhost:27017/tutorial',
                'collection' : 'users'
            }
        })

        # Test * no where
        # Test * where
        # Test cols no where
        # Test cols where
        mldb.query("SELECT * FROM ds WHERE username='Finch'")

    def test_dataset_missing_param(self):
        msg = 'connectionScheme is a required property'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/datasets/ds_err1', {
                'type' : 'mongodb.dataset',
                'params' : {
                    'collection' : 'users'
                }
            })
        msg = 'collection is a required property and must not be empty'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put('/v1/datasets/ds_err2', {
                'type' : 'mongodb.dataset',
                'params' : {
                    'connectionScheme' : 'mongodb://localhost:27017/tutorial'
                }
            })


if __name__ == '__main__':
    mldb.run_tests()
