#
# where-without-dataset.py
# 2017-01-09
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#
if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa

class WhereWithoutDatasetTest(MldbUnitTest):  # noqa

    def test_where_false(self):
        res = mldb.query("SELECT 1 WHERE false")
        self.assertTableResultEquals(res, [
            [
                "_rowName"
            ]
        ])

    def test_where_limit_0(self):
        res = mldb.query("SELECT 1 LIMIT 0")
        self.assertTableResultEquals(res, [
            [
                "_rowName"
            ]
        ])

    def test_where_limit_1(self):
        res = mldb.query("SELECT 1 LIMIT 1")
        self.assertTableResultEquals(res, [
            [ "_rowName", "1" ],
            [ "result", 1]
        ])
        
mldb.run_tests()
