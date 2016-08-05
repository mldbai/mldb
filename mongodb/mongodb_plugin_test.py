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

    @unittest.skip("Requires local mldb")
    def test_record(self):
        # Example of dataset mongodb.record.
        # Pymongo should be used to assert the test wrote properly.
        res = mldb.create_dataset({
            'id' : 'ds',
            'type' : 'mongodb.record',
            'params' : {
                'connectionScheme' : 'mongodb://localhost:27017/tutorial',
                'collection' : 'newb',
            }
        })
        res.record_row('monNom', [['colA', 'valeur sure', 34]])
        res.commit()

    @unittest.skip("Requires local mldb + data to query")
    def test_query(self):
        # Example of a query passed straight to mongodb. The result comes back
        # formatted as an MLDB result.
        res = mldb.put('/v1/functions/mongo_query', {
            'type' : 'mongodb.query',
            'params' : {
                'connectionScheme' : 'mongodb://localhost:27017/tutorial',
                'collection' : 'users'
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
        pass

if __name__ == '__main__':
    mldb.run_tests()
