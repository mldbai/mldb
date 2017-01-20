# -*- coding: utf-8 -*-
#
# fetcher-function.py
# Francois-Michel L'Heureux, 2016-09-13
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

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


if __name__ == '__main__':
    mldb.run_tests()

