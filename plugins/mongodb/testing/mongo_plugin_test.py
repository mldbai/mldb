# -*- coding: utf-8 -*-
#
# mongodb_plugin_test.py
# Mich, 2016-08-02
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest
import json
import subprocess
import datetime
import time
from dateutil.tz import tzutc
try:
    subprocess.check_call(['which', 'mongod'])
    got_mongod = True
except subprocess.CalledProcessError:
    got_mongod = False

if got_mongod:
    import sys
    sys.path.append('build/x86_64/bin')
    from python_mongo_temp_server_wrapping import MongoTemporaryServerPtr
    from pymongo import MongoClient

if False:
    mldb_wrapper = None

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MongodbPluginTest(MldbUnitTest):  # noqa

    port = 0

    @classmethod
    def setUpClass(cls):
        if got_mongod:
            # We use a single server instance for the whole test
            cls.mongo_tmp_server = MongoTemporaryServerPtr("", 0)
            cls.port = cls.mongo_tmp_server.get_port_num()
            cls.pymongo = MongoClient('localhost', cls.port)
            cls.connection_scheme = \
                'mongodb://localhost:{}/test_db'.format(cls.port)
            cls.collection_name = 'test_collection'
            cls.pymongo_db = cls.pymongo.test_db
            coll = cls.pymongo_db.test_collection
            coll.insert_one({
                'type' : 'simple'
            })
            coll.insert_one({
                'notype' : None
            })
            coll.insert_one({
                'type' : 'nested_obj',
                'obj' : {
                    'a' : {'b' : 'c'},
                    'd' : 'e'
                }
            })
            coll.insert_one({
                'type' : 'nested_arr',
                'arr' : [1, 2, [3, 4], 5]
            })

            cls.non_std_id_coll_name = 'non_std_id_coll'
            cls.pymongo_db.non_std_id_coll.insert_one({
                '_id' : {'compound' : 'key id'},
                'data' : 'foo braque'
            })

    @classmethod
    def tearDownClass(cls):
        if got_mongod:
            del cls.pymongo_db
            del cls.mongo_tmp_server
            cls.pymongo.close()
            del cls.pymongo

            # Leave time to close properly, if not it causes the error
            # Py_EndInterpreter: not the last thread
            time.sleep(1)

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

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_import(self):
        """
        Example of import procedure. select, named, where, limit and offset
        are supported options.
        """
        res = mldb.post('/v1/procedures', {
            'type' : 'mongodb.import',
            'params' : {
                'uriConnectionScheme' : self.connection_scheme,
                'collection' : self.collection_name,
                'outputDataset' : {
                    'id' : 'imported',
                    'type' : 'sparse.mutable'
                }
            }
        })

        # mongo auto generated id (here becoming rowName) are always
        # generated in increasing order
        res = mldb.get('/v1/query',
                       q="SELECT * FROM imported ORDER BY rowName()").json()

        def find_id_idx(columns):
            for idx, c in enumerate(columns):
                if c[0] == '_id':
                    return idx
            raise Exception('_id not found')

        # test _rowName == _id, and substitute rowName to idx for easier
        # further assertions
        dates = []
        for row_idx, r in enumerate(res):
            col_idx = find_id_idx(r['columns'])
            self.assertEqual(r['rowName'], r['columns'][col_idx][1])

            # extrach epoch from the mongo object id
            epoch = int(r['columns'][col_idx][1][:8], 16)

            iso_date = \
                datetime.datetime.fromtimestamp(epoch, tzutc()).isoformat()
            dates.append(iso_date[:-6] + 'Z') # replace +00:00 with Z
            r['columns'][col_idx][1] = row_idx
            r['rowName'] = row_idx

        self.assertFullResultEquals(res, [
            {
                "rowName": 0,
                "columns": [["_id", 0, dates[0]],["type", "simple", dates[0]]]
            },
            {
                "rowName": 1,
                "columns": [ ["_id", 1, dates[1]], ["notype", None, dates[1]]]
            },
            {
                "rowName": 2,
                "columns": [["_id", 2, dates[2]],
                            ["obj.a.b", "c", dates[2]],
                            ["obj.d", "e", dates[2]],
                            ["type", "nested_obj", dates[2]]]
            },
            {
                "rowName": 3,
                "columns": [["_id", 3, dates[3]],
                            ["arr.0", 1, dates[3]],
                            ["arr.1", 2, dates[3]],
                            ["arr.2.0", 3, dates[3]],
                            ["arr.2.1", 4, dates[3]],
                            ["arr.3", 5, dates[3]],
                            ["type", "nested_arr", dates[3]]]
            }
        ])

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_import_oid(self):
        """
        Example of import procedure. select, named, where, limit and offset
        are supported options.
        """
        mldb.post('/v1/procedures', {
            'type' : 'mongodb.import',
            'params' : {
                'uriConnectionScheme' : self.connection_scheme,
                'collection' : self.collection_name,
                'outputDataset' : {
                    'id' : 'imported_oid',
                    'type' : 'sparse.mutable'
                },
                'select' : 'oid()'
            }
        })
        res = mldb.query("SELECT * FROM imported_oid")
        self.assertEqual(len(res[0]), 2,
                         "columns should be _rowName and oid()")
        for r in res[1:]:
            self.assertEqual(r[0], r[1])

    def test_invalid_connection_scheme(self):
        msg = 'the minimal uriConnectionScheme format is'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post('/v1/procedures', {
                'type' : 'mongodb.import',
                'params' : {
                    'uriConnectionScheme' : 'mongodb://',
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
                    'uriConnectionScheme' : 'bouette://',
                    'collection' : 'users',
                    'outputDataset' : {
                        'id' : 'out',
                        'type' : 'sparse.mutable'
                    }
                }
            })

        msg = 'uriConnectionScheme is a required property'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post('/v1/procedures', {
                'type' : 'mongodb.import',
                'params' : {
                    'uriConnectionScheme' : '',
                    'collection' : 'users',
                    'outputDataset' : {
                        'id' : 'out',
                        'type' : 'sparse.mutable'
                    }
                }
            })

    def test_import_missing_param(self):
        msg = 'uriConnectionScheme is a required property'
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
                    'uriConnectionScheme' : 'mongodb://localhost:27017/tutorial',
                    'outputDataset' : {
                        'id' : 'out',
                        'type' : 'sparse.mutable'
                    }
                }
            })

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_record(self):
        """
        Example of dataset mongodb.record.
        """
        res = mldb.create_dataset({
            'id' : 'ds_record',
            'type' : 'mongodb.record',
            'params' : {
                'uriConnectionScheme' : self.connection_scheme,
                'collection' : 'record'
            }
        })
        res.record_row('row1', [['colA', 'valeur sure', 34]])
        res.record_row('dotted.row2', [
            ['colA', 'other valeur sure', 3],
            ['space colB', 43, 4],
            ['true', True, 1],
            ['false', False, 1]
        ])
        res.record_row('"quoted"', [['whatever', 1, 5]])

        res = self.pymongo_db.record.find()
        rows = [r for r in res]
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], {
            '_id' : 'row1',
            'colA' : 'valeur sure'
        })
        self.assertEqual(rows[1], {
            '_id' : '"dotted.row2"',
            'colA' : 'other valeur sure',
            'space colB' : 43,
            'true' : 1,
            'false' : 0
        })

        self.assertEqual(rows[2]['_id'], '"""quoted"""')

    def test_record_missing_params(self):
        msg = 'uriConnectionScheme is a required property'
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
                    'uriConnectionScheme' : 'mongodb://localhost:27017/tutorial'
                }
            })

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_query_first_row(self):
        # Example of a query passed straight to mongodb. The result comes back
        # formatted as an MLDB result.
        mldb.put('/v1/functions/mongo_query', {
            'type' : 'mongodb.query',
            'params' : {
                'uriConnectionScheme' : self.connection_scheme,
                'collection' : 'test_collection'
            }
        })
        query = json.dumps({
            'type' : {
                '$eq' : 'nested_obj'
            }
        })
        res = mldb.get('/v1/functions/mongo_query/application',
                       input={'query' : query}).json()
        self.assertEqual(res['output']['type'], 'nested_obj')

        _id = res['output']['_id']
        query = json.dumps({
            '_id' : _id
        })
        res = mldb.get('/v1/functions/mongo_query/application',
                       input={'query' : query}).json()
        self.assertEqual(res['output']['type'], 'nested_obj')

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_query_first_row_bad_oid(self):
        # _id of length are tried as ObjectIDs. Make sure they don't cause
        # issue even when they can't be converted to ObjectIDs.
        mldb.put('/v1/functions/mongo_query_bad_oid', {
            'type' : 'mongodb.query',
            'params' : {
                'uriConnectionScheme' : self.connection_scheme,
                'collection' : 'test_collection'
            }
        })
        query = json.dumps({
            '_id' : 'Z' * 12
        })
        mldb.get('/v1/functions/mongo_query_bad_oid/application',
                 input={'query' : query}).json()

        query = json.dumps({
            '_id' : 'Z' * 24
        })
        mldb.get('/v1/functions/mongo_query/application',
                 input={'query' : query}).json()

    def test_query_first_row_missing_param(self):
        msg = 'uriConnectionScheme is a required property'
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
                    'uriConnectionScheme' : 'mongodb://localhost:27017/tutorial'
                }
            })

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_query_named_row(self):
        # Example of a query passed straight to mongodb. The result comes back
        # formatted as an MLDB result.
        mldb.put('/v1/functions/mongo_query', {
            'type' : 'mongodb.query',
            'params' : {
                'uriConnectionScheme' : self.connection_scheme,
                'collection' : 'test_collection',
                'output' : 'NAMED_COLUMNS'
            }
        }),
        query = json.dumps({
            'username' : {
                '$ne' : 'simple'
            }
        })
        res = mldb.get('/v1/functions/mongo_query/application',
                       input={'query' : query}).json()
        keys = res['output'].keys()
        keys.sort()
        self.assertEqual(res['output'][keys[0]][1][0], 'type')
        self.assertEqual(res['output'][keys[0]][1][1][0], 'simple')

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_query_no_query(self):
        # Example of a query passed straight to mongodb. The result comes back
        # formatted as an MLDB result.
        mldb.put('/v1/functions/mongo_query_no_query', {
            'type' : 'mongodb.query',
            'params' : {
                'uriConnectionScheme' : self.connection_scheme,
                'collection' : 'test_collection'
            }
        })
        msg = 'You must define the parameter \\\\"query\\\\"'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.get('/v1/functions/mongo_query_no_query/application')

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_dataset(self):
        # Example of a read only mongo db dataset. MLDB queries can be made
        # over it.
        mldb.put('/v1/datasets/ds', {
            'type' : 'mongodb.dataset',
            'params' : {
                'uriConnectionScheme' : self.connection_scheme,
                'collection' : 'test_collection',
            }
        })

        res = mldb.query("SELECT * FROM ds")
        self.assertEqual(len(res), 5)

        res = mldb.query("SELECT * FROM ds WHERE unexisting_field='Finch'")
        self.assertEqual(len(res), 1)
        res = mldb.query("SELECT * FROM ds WHERE type='simple'")
        self.assertEqual(len(res), 2)
        self.assertEqual(res[1][2], 'simple')

        res = mldb.query("SELECT type FROM ds ORDER BY type")
        self.assertEqual(res[1][1], None)
        self.assertEqual(res[2][1], 'nested_arr')
        self.assertEqual(res[3][1], 'nested_obj')
        self.assertEqual(res[4][1], 'simple')

        res = mldb.query("SELECT username FROM ds WHERE unexisting != 'Finch'")
        self.assertEqual(len(res), 1)

        res = mldb.query("SELECT username FROM ds WHERE type != 'simple'")
        self.assertEqual(len(res), 3)

    def test_dataset_missing_param(self):
        msg = 'uriConnectionScheme is a required property'
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
                    'uriConnectionScheme' : 'mongodb://localhost:27017/tutorial'
                }
            })

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_record_invalid_keys(self):
        ds = mldb.create_dataset({
            'id' : 'ds_record_invalid',
            'type' : 'mongodb.record',
            'params' : {
                'uriConnectionScheme' : self.connection_scheme,
                'collection' : 'record_invalid'
            }
        })

        msg = 'Dotted keys cannot be recorded to MongoDB'
        with self.assertRaisesRegexp(RuntimeError, msg):
            ds.record_row('row1', [['dotted.keyΏ', 1, 1]])

        msg = 'Keys starting with a dollar sign cannot be recorded'
        with self.assertRaisesRegexp(RuntimeError, msg):
            ds.record_row('row1', [['$dollarsignΏ', 1, 1]])

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_double_record(self):
        """
        Recording two rows with the same name
        """
        ds = mldb.create_dataset({
            'id' : 'ds_double_record',
            'type' : 'mongodb.record',
            'params' : {
                'uriConnectionScheme' : self.connection_scheme,
                'collection' : 'record_double'
            }
        })
        ds.record_row('rowA', [['colA', 1, 1]])

        with self.assertRaisesRegexp(RuntimeError, "duplicate key error"):
            ds.record_row('rowA', [['colB', 2, 1]])

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_dataset_on_non_std_id(self):
        mldb.put('/v1/datasets/non_std_id_ds', {
            'type' : 'mongodb.dataset',
            'params' : {
                'uriConnectionScheme' : self.connection_scheme,
                'collection' : self.non_std_id_coll_name,
            }
        })

        msg = "unimplemented support"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT * FROM non_std_id_ds")

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_import_on_non_std_id(self):
        msg = "unimplemented support"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.post('/v1/procedures', {
                'type' : 'mongodb.import',
                'params' : {
                    'uriConnectionScheme' : self.connection_scheme,
                    'collection' : self.non_std_id_coll_name,
                    'outputDataset' : {
                        'id' : 'imported',
                        'type' : 'sparse.mutable'
                    }
                }
            })

    @unittest.skipIf(not got_mongod, "mongod not available")
    def test_query_on_non_std_id(self):
        mldb.put('/v1/functions/mongo_query_invalid', {
            'type' : 'mongodb.query',
            'params' : {
                'uriConnectionScheme' : self.connection_scheme,
                'collection' : self.non_std_id_coll_name,
            }
        })
        query = json.dumps({
            'data' : {
                '$eq' : 'foo braque'
            }
        })
        mldb.get('/v1/functions/mongo_query_invalid/application',
                 input={'query' : query}).json()

if __name__ == '__main__':
    mldb.run_tests()
