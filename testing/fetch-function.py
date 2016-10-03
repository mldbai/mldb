#
# fetch-function.py
# Francois-Michel L'Heureux, 2016-09-13
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class FetcherFunction(MldbUnitTest):  # noqa

    def test_non_builtin(self):
        mldb.put('/v1/functions/my_fetch', { 'type': 'fetch' })

        mldb.put('/v1/functions/getCountryNonBuiltin', {
            'type': 'sql.expression',
            'params': {
                'expression':
                    "extract_column('geoplugin_countryCode', parse_json(CAST "
                    "(my_fetch({url: 'http://www.geoplugin.net/json.gp?ip=' + ip})[content] AS STRING))) as country"
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
                    "(fetch('http://www.geoplugin.net/json.gp?ip=' + ip)[content] AS STRING))) as country"
            }
        })

        res = mldb.query(
            "SELECT getCountryBuiltin({ip: '158.245.13.123'}) AS *")
        self.assertTableResultEquals(res, [
            ['_rowName', 'country'],
            ['result', 'US']
        ])

        res = mldb.get('/v1/query',
            q="SELECT fetch('http://www.geoplugin.net/json.gp?ip=158.245.13.123') AS *").json()
        cols = res[0]['columns']
        self.assertTrue('content' in [cols[0][0], cols[1][0]])
        self.assertTrue('error' in [cols[0][0], cols[1][0]])


if __name__ == '__main__':
    mldb.run_tests()

