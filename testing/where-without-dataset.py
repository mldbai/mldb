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
        
mldb.run_tests()
