# -*- coding: utf-8 -*-
#
# MLDB-907-tokenize.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


class TokenizeTest(MldbUnitTest):  # noqa

    def find_column(cls, result, column, value):
        response = result.json()
        num_row = len(response)
        for row_index in range(num_row):
            row = response[row_index]['columns']
            for col_index in range(len(row)):
                if row[col_index][0] == column:
                    cls.assertEqual(row[col_index][1], value)
                    return
        raise Exception("Col not found: " + column)

    def not_find_column(cls, result, column):
        response = result.json()
        num_row = len(response)
        for row_index in range(num_row):
            row = response[row_index]['columns']
            for col_index in range(len(row)):
                cls.assertNotEqual(row[col_index][0], column)


    def test_base(self):
        result = mldb.get('/v1/query',
                          q="SELECT tokenize('a,b,c,a') AS tokens")
        self.find_column(result, 'tokens.a', 2)
        self.find_column(result, 'tokens.c', 1)
        self.find_column(result, 'tokens.b', 1)

    def test_splitChars(self):
        result = mldb.get(
            '/v1/query',
            q=u"SELECT tokenize('a b c a', {' …' AS splitChars}) AS tokens")
        self.find_column(result, 'tokens.a', 2)
        self.find_column(result, 'tokens.c', 1)
        self.find_column(result, 'tokens.b', 1)

    def test_in_transform(self):
        mldb.post('/v1/procedures', {
            'type': 'transform',
            'params': {
                'inputData': u"""
                    SELECT
                        tokenize(
                            'abouyayaa adsf 2 ; sdv, sdf',
                            {splitChars: ',; …', minTokenLength: 3} 
                        ) as *
                    """,
                'outputDataset': 'bag_of_words',
                'runOnCreation': True
            }
        })

        self.assertTableResultEquals(
            mldb.query("select * from bag_of_words"),
            [["_rowName","abouyayaa","adsf","sdf","sdv"],
             ["result",1,1,1,1]])

    def test_splitChars_and_str_value(self):
        result = mldb.get(
            '/v1/query',
            q="SELECT tokenize('a b c a', {' ' AS splitChars, 'found' AS value}) AS tokens")
        self.find_column(result, 'tokens.a', 'found')
        self.find_column(result, 'tokens.b', 'found')
        self.find_column(result, 'tokens.c', 'found')

    def test_splitChars_int_as_value(self):
        """MLDB-1338"""
        result = mldb.get(
            '/v1/query',
            q="SELECT tokenize('a b c a', {' ' AS splitChars, 1 AS value}) AS tokens")
        self.find_column(result, 'tokens.a', 1)
        self.find_column(result, 'tokens.b', 1)
        self.find_column(result, 'tokens.c', 1)

    def test_splitChars_and_quoteChar(self):
        result = mldb.get(
            '/v1/query',
            q="""SELECT tokenize('a,b,$c,a$,a',
                                 {'$' AS quoteChar, ',' AS splitChars})
                        AS tokens""")
        self.find_column(result, 'tokens.a', 2)
        self.find_column(result, 'tokens.c,a', 1)
        self.find_column(result, 'tokens.b', 1)

    def test_2_splitChars(self):
        """MLDB-1018. try with 2 splitChars."""
        result = mldb.get(
            '/v1/query',
            q="SELECT tokenize('a,b,$c,a$,a', {',$' AS splitChars}) AS tokens")
        self.find_column(result, 'tokens.a', 3)
        self.find_column(result, 'tokens.c', 1)
        self.find_column(result, 'tokens.b', 1)

    def test_splitChars_and_limit_and_offset(self):
        result = mldb.get(
            '/v1/query',
            q="""SELECT tokenize('a b c a',
                                 {' ' AS splitChars, 2 AS limit, 1 AS offset})
                        AS tokens""")
        self.find_column(result, 'tokens.c', 1)
        self.find_column(result, 'tokens.b', 1)
        self.not_find_column(result, 'tokens.a')

    def test_token_extract(self):
        result = mldb.get('/v1/query',
                          q="SELECT token_extract('a,b,c,a', 2) AS token")
        self.assertEqual(result.json()[0]['columns'][0][1], "c")

    def test_token_extract_splitChars_and_limit_and_offset(self):
        result = mldb.get(
            '/v1/query',
            q="""
            SELECT token_extract('a b c d e f', 3,
                                 {' ' AS splitChars, 2 AS limit, 1 AS offset})
                   AS token""")
        self.assertEqual(result.json()[0]['columns'][0][1], "e")

        result = mldb.get(
            '/v1/query',
            q="""
            SELECT token_extract('a b c d e f', -1,
                                 {' ' AS splitChars, 2 AS limit, 1 AS offset})
                   AS token""")
        self.assertEqual(result.json()[0]['columns'][0][1], "c")

    def test_tokenize_utf8(self):
        result = mldb.get(
            '/v1/query',
            q=unicode("SELECT tokenize('à,bay,ç,day') AS tokens",
                      encoding='utf-8'))
        self.find_column(result, unicode('tokens.à', encoding='utf-8'), 1)
        self.find_column(result, unicode('tokens.bay', encoding='utf-8'), 1)
        self.find_column(result, unicode('tokens.ç', encoding='utf-8'), 1)
        self.find_column(result, unicode('tokens.day', encoding='utf-8'), 1)

    def test_tokenize_legacy_args(self):
        self.assertTableResultEquals(
            mldb.query("""
                SELECT tokenize('s y z hoho bouya "pwet zou"', 
                            {min_token_length:2, ngram_range:[1, 2], 
                             splitchars:' ', quotechar: '"'}) as *
                """),
            [["_rowName","bouya","bouya_pwet zou","hoho","hoho_bouya","pwet zou"],
             ["result",1,1,1,1,1]])

    def test_tokenize_min_token_length(self):
        result = mldb.get(
            '/v1/query',
            q="""SELECT tokenize('I would want a burger',
                                 {splitChars: ' ', minTokenLength:2})
                 AS tokens""")
        self.find_column(result, 'tokens.would', 1)
        self.find_column(result, 'tokens.want', 1)
        self.find_column(result, 'tokens.burger', 1)
        self.not_find_column(result, 'tokens.I')

    def test_tokenize_ngram_range(self):
        result = mldb.get(
            '/v1/query',
            q="""SELECT tokenize('I would want a burger',
                                 {splitChars: ' ', ngramRange: [1, 3],
                                  minTokenLength:2})
                        AS tokens""")
        self.find_column(result, "tokens.would_want_burger", 1)
        self.find_column(result, "tokens.burger", 1)
        self.find_column(result, "tokens.would_want", 1)
        self.find_column(result, "tokens.want", 1)
        self.find_column(result, "tokens.want_burger", 1)
        self.find_column(result, "tokens.would", 1)

        result = mldb.get(
            '/v1/query',
            q="""SELECT tokenize('I would want a burger I would want a burger',
                                 {splitChars: ' ', ngramRange: [3, 3], minTokenLength:2})
                        AS tokens""")
        self.find_column(result, "tokens.would_want_burger", 2)
        self.not_find_column(result, "tokens.would_want")

        result = mldb.get(
            '/v1/query',
            q="""SELECT tokenize('I would want a burger',
                                 {splitChars: ' ', ngramRange: [1, 2]})
                        AS tokens""")
        self.find_column(result, "tokens.I_would", 1)

    def test_tokenize_failure(self):
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.get(
                '/v1/query',
                q="""SELECT tokenize('I would want a burger',
                                     {splitChars: ' ', ngramRange: [-2, 8]})
                            AS tokens""")

    def test_tokenize_to_print_json(self):
        result = mldb.get(
            '/v1/query',
            q="""SELECT print_json(tokenize('I.am.a dog and.this is.my life.',
                                            {splitChars: ' '}))
                        AS tokens""")

        self.find_column(result, "tokens",
                    '{"I.am.a":1,"and.this":1,"dog":1,"is.my":1,"life.":1}')

    def test_tokenize_null(self):
        """
        MLDB-1726
        """
        res = mldb.get('/v1/query',
                       q="SELECT tokenize(NULL) NAMED 'res'").json()
        self.assertFullResultEquals(res, [{
            'rowName' : 'res',
            'columns' : [['tokenize(NULL)', None, '-Inf']]
        }])

if __name__ == '__main__':
    mldb.run_tests()
