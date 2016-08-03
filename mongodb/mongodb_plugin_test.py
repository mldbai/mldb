#
# mongodb_plugin_test.py
# Mich, 2016-08-02
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MongodbPluginTest(MldbUnitTest):  # noqa

    def test_import_available(self):
        res = mldb.get('/v1/types/procedures')
        self.assertTrue('mongodb.import' in res.json())

    def test_record_available(self):
        res = mldb.get('/v1/types/datasets')
        self.assertTrue('mongodb.record' in res.json())



if __name__ == '__main__':
    mldb.run_tests()
