# MLDB-1935-const-optim.py
# Mathieu Marquis Bolduc, 2016-12-15
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest
import time

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


class ConstOptimTest(MldbUnitTest):
    @classmethod
    def setUpClass(self):
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        for num in range(0,2000):
            ds.record_row("a" + str(num),[["x", num, 0]])
        ds.commit()

    def test_fetcher_call(self):

        mldb.put('/v1/functions/fetch', {
            "type": 'fetcher',
            "params": {}
        })

        startTime = time.clock()
        mldb.query('SELECT blob_length(fetch({\'file://mldb/testing/logo-new.jpg\' as url})[content]) as x')
        deltaT = time.clock() - startTime
        mldb.log(deltaT)

        startTime = time.clock()
        mldb.query('SELECT x, blob_length(fetch({\'file://mldb/testing/logo-new.jpg\' as url})[content]) as y FROM sample')
        optimizedDeltaT = time.clock() - startTime
        mldb.log(optimizedDeltaT)

        mldb.put('/v1/functions/fetch2', {
            "type": 'fetcher',
            "params": {},
            "deterministic" : False,
        })

        startTime = time.clock()
        mldb.query('SELECT x, blob_length(fetch2({\'file://mldb/testing/logo-new.jpg\' as url})[content]) as y FROM sample')
        nonOptimizedDeltaT = time.clock() - startTime
        mldb.log(deltaT)

        self.assertTrue(nonOptimizedDeltaT > optimizedDeltaT)

mldb.run_tests()
