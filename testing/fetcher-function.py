# -*- coding: utf-8 -*-
#
# fetcher-function.py
# Francois-Michel L'Heureux, 2016-09-13
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import subprocess
import time
import prctl
import signal
import os
from socket import socket

mldb = mldb_wrapper.wrap(mldb)  # noqa

class FetcherFunction(MldbUnitTest):  # noqa

    def test_non_builtin(self):
        mldb.put('/v1/functions/fetch', { 'type': 'fetcher' })

        mldb.put('/v1/functions/getCountryNonBuiltin', {
            'type': 'sql.expression',
            'params': {
                'expression':
                    "extract_column('geoplugin_countryCode', parse_json(CAST "
                    "(fetch({url: 'http://www.geoplugin.net/json.gp?ip=' + ip})[content] AS STRING))) as country"
            }
        })

        res = mldb.query(
            "SELECT getCountryNonBuiltin({ip: '158.245.13.123'}) AS *")
        self.assertTableResultEquals(res, [
            ['_rowName', 'country'],
            ['result', 'US']
        ])

    def test_builtin(self):
        mldb.put('/v1/functions/getCountryBuiltin', {
            'type': 'sql.expression',
            'params': {
                'expression':
                    "extract_column('geoplugin_countryCode', parse_json(CAST "
                    "(fetcher('http://www.geoplugin.net/json.gp?ip=' + ip)[content] AS STRING))) as country"
            }
        })

        res = mldb.query(
            "SELECT getCountryBuiltin({ip: '158.245.13.123'}) AS *")
        self.assertTableResultEquals(res, [
            ['_rowName', 'country'],
            ['result', 'US']
        ])

        res = mldb.get('/v1/query',
            q="SELECT fetcher('http://www.geoplugin.net/json.gp?ip=158.245.13.123') AS *").json()
        cols = res[0]['columns']
        self.assertTrue('content' in [cols[0][0], cols[1][0]])
        self.assertTrue('error' in [cols[0][0], cols[1][0]])

    def test_builtin_utf8(self):
        # MLDB-2098

        # can't put it on aws because é becomes %C3%A9
        res = mldb.query("SELECT fetcher('file://mldb/testing/utéf8.png')")
        self.assertGreater(len(res[1][1]['blob']), 10)
        self.assertTrue(res[1][2] is None)

    def test_builtin_utf8_unexisting(self):
        # there was an issue when an unfound file had utf-8 chars
        res = mldb.query("SELECT fetcher('file://mldb/testing/utéf8_unexisting.jpg')")
        self.assertTrue(res[1][1] is None)
        self.assertTrue(type(res[1][2]) is unicode)

    def test_non_builtin_max_concurrency_bad_param(self):
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.post('/v1/functions', {
                'type': 'fetcher',
                'params' : {
                    'maxConcurrentFetch': -2
                }
            })

        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.post('/v1/functions', {
                'type': 'fetcher',
                'params' : {
                    'maxConcurrentFetch': -0
                }
            })

    def test_non_builtin_max_concurrency(self):
        # setup test server
        s = socket()
        s.bind(('', 0))
        port = s.getsockname()[1]
        s.close()

        def pre_exec():
            # new process group - all our child will be in that group
            prctl.set_pdeathsig(signal.SIGTERM)
            os.setpgid(0, 0)

        proc = subprocess.Popen(['/usr/bin/env', 'python',
                                 'mldb/testing/test_server.py', str(port)],
                                preexec_fn=pre_exec)

        for _ in xrange(10):
            time.sleep(1)
            res = mldb.query(
                "SELECT fetcher('http://localhost:{}/200')[error]".format(port))
            if (res[1][1] is None):
                # test server ready
                break
        else:
            raise RuntimeError("Failed to connect to test server")

        ds = mldb.create_dataset({
            'id' : 'fetcher_concurrency_test',
            'type' : 'sparse.mutable'
        })
        url = 'http://localhost:{}/sleep/2'.format(port)
        ds.record_row('r1', [['f', url, 0]])
        ds.record_row('r2', [['f', url, 0]])
        ds.commit()

        mldb.put('/v1/functions/single_fetch', {
            'type': 'fetcher',
            'params' : {
                'maxConcurrentFetch': 1
            }
        })

        start = time.time()
        res = mldb.query(
            "SELECT single_fetch({url: f}) FROM fetcher_concurrency_test")
        duration = time.time() - start
        self.assertGreaterEqual(duration, 4)

        mldb.put('/v1/functions/regular_fetch', {
            'type': 'fetcher'
        })
        start = time.time()
        res = mldb.query(
            "SELECT regular_fetch({url: f}) FROM fetcher_concurrency_test")
        duration = time.time() - start
        self.assertLess(duration, 4)

        mldb.put('/v1/functions/explicit_regular_fetch', {
            'type': 'fetcher',
            'params' : {
                'maxConcurrentFetch' : -1
            }
        })
        start = time.time()
        res = mldb.query("SELECT explicit_regular_fetch({url: f}) "
                         "FROM fetcher_concurrency_test")
        duration = time.time() - start
        self.assertLess(duration, 4)

        proc.terminate()


if __name__ == '__main__':
    mldb.run_tests()

