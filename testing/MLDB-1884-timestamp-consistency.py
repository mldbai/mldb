#
# MLDB-1884-timestamp-consistency.py
# Francois-Michel L Heureux, 2016-08-12
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1884TimestampConsistency(MldbUnitTest):  # noqa

    def test_null(self):
        res = mldb.get('/v1/query', q="SELECT null")
        mldb.log(res)

    def test_str(self):
        res = mldb.get('/v1/query', q="SELECT 'patate'")
        mldb.log(res)

    def test_operator(self):
        res = mldb.get('/v1/query', q="SELECT NULL LIKE 'abc'")
        mldb.log(res)

if __name__ == '__main__':
    mldb.run_tests()
