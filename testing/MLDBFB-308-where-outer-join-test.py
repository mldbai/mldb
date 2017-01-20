#
# MLDBFB-308-where-outer-join-test.py
# Mich, 2016-01-13
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class WhereOuterJoinTest(MldbUnitTest):

    def test_it(self):
        url = '/v1/datasets/ds'
        mldb.put(url, {
            'type' : 'sparse.mutable',
        })

        mldb.post(url + '/rows', {
            'rowName' : 'userValid',
            'columns' : [['behA', 1, 3]]
        })

        mldb.post(url + '/commit')

        mldb.query(
            "SELECT 1 FROM ds OUTER JOIN (SELECT 2 FROM ds) WHERE behA")

if __name__ == '__main__':
    mldb.run_tests()
