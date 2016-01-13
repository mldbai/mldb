# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json

def find_column(result, column, value):
	assert(result['statusCode'] == 200)
	response = json.loads(result['response'])
	numRow = len(response)
	found = False
	for rowIndex in range (0, numRow):
		row = response[rowIndex]['columns']
		numCol = len(row)
		for colIndex in range (0, numCol):
			if row[colIndex][0] == column:
				found = True
				assert row[colIndex][1] == value
				break
		if (found):
			break
	assert found

def not_find_column(result, column):
	assert(result['statusCode'] == 200)
	response = json.loads(result['response'])
	numRow = len(response)
	for rowIndex in range (0, numRow):
		row = response[rowIndex]['columns']
		numCol = len(row)
		for colIndex in range (0, numCol):
			assert row[colIndex][0] != column

result = mldb.perform('GET', '/v1/query', [['q', "SELECT tokenize('a,b,c,a') as tokens"]])
mldb.log(result)
find_column(result, 'tokens.a', 2)
find_column(result, 'tokens.c', 1)
find_column(result, 'tokens.b', 1)


result = mldb.perform('GET', '/v1/query', [['q', "SELECT tokenize('a b c a', {' ' as splitchars}) as tokens"]])
mldb.log(result)
find_column(result, 'tokens.a', 2)
find_column(result, 'tokens.c', 1)
find_column(result, 'tokens.b', 1)


result = mldb.perform('GET', '/v1/query', [['q', "SELECT tokenize('a b c a', {' ' as splitchars, 'found' as value}) as tokens"]])
mldb.log(result)
find_column(result, 'tokens.a', 'found')
find_column(result, 'tokens.b', 'found')
find_column(result, 'tokens.c', 'found')

result = mldb.perform('GET', '/v1/query', [['q', "SELECT tokenize('a,b,$c,a$,a', {'$' as quotechar, ',' as splitchars}) as tokens"]])
mldb.log(result)
find_column(result, 'tokens.a', 2)
find_column(result, 'tokens.c,a', 1)
find_column(result, 'tokens.b', 1)

# MLDB-1018. try with 2 splitchars
result = mldb.perform('GET', '/v1/query', [['q', "SELECT tokenize('a,b,$c,a$,a', {',$' as splitchars}) as tokens"]])
mldb.log(result)
find_column(result, 'tokens.a', 3)
find_column(result, 'tokens.c', 1)
find_column(result, 'tokens.b', 1)

result = mldb.perform('GET', '/v1/query', [['q', "SELECT tokenize('a b c a', {' ' as splitchars, 2 as limit, 1 as offset}) as tokens"]])
mldb.log(result)
find_column(result, 'tokens.c', 1)
find_column(result, 'tokens.b', 1)
not_find_column(result, 'tokens.a')

result = mldb.perform('GET', '/v1/query', [['q', "SELECT token_extract('a,b,c,a', 2) as token"]])
mldb.log(result)
assert result['statusCode'] == 200
assert json.loads(result['response'])[0]['columns'][0][1] == "c"

result = mldb.perform('GET', '/v1/query', [['q', "SELECT token_extract('a b c d e f', 3, {' ' as splitchars, 2 as limit, 1 as offset}) as token"]])
mldb.log(result)
assert result['statusCode'] == 200
assert json.loads(result['response'])[0]['columns'][0][1] == "e"

result = mldb.perform('GET', '/v1/query', [['q', "SELECT token_extract('a b c d e f', -1, {' ' as splitchars, 2 as limit, 1 as offset}) as token"]])
mldb.log(result)
assert result['statusCode'] == 200
assert json.loads(result['response'])[0]['columns'][0][1] == "c"

result = mldb.perform('GET', '/v1/query', [['q', "SELECT tokenize('à,bay,ç,day') as tokens"]])
mldb.log(result)
find_column(result, unicode('tokens.à',encoding='utf-8'), 1)
find_column(result, unicode('tokens.bay',encoding='utf-8'), 1)
find_column(result, unicode('tokens.ç',encoding='utf-8'), 1)
find_column(result, unicode('tokens.day',encoding='utf-8'), 1)


result = mldb.perform('GET', '/v1/query', [['q', "SELECT tokenize('I would want a burger', {splitchars: ' ', min_token_length:2}) as tokens"]])
mldb.log(result)
find_column(result, 'tokens.would', 1)
find_column(result, 'tokens.want', 1)
find_column(result, 'tokens.burger', 1)
not_find_column(result, 'tokens.I')


result = mldb.perform('GET', '/v1/query', [['q', "SELECT tokenize('I would want a burger', {splitchars: ' ', ngram_range: {1, 3}, min_token_length:2}) as tokens"]])
jsRez = json.loads(result["response"])
mldb.log(jsRez)
find_column(result, "tokens.would_want_burger", 1)
find_column(result, "tokens.burger", 1)
find_column(result, "tokens.would_want", 1)
find_column(result, "tokens.want", 1)
find_column(result, "tokens.want_burger", 1)
find_column(result, "tokens.would", 1)

result = mldb.perform('GET', '/v1/query', [['q', "SELECT tokenize('I would want a burger I would want a burger', {splitchars: ' ', ngram_range: {3, 3}, min_token_length:2}) as tokens"]])
jsRez = json.loads(result["response"])
mldb.log(jsRez)
find_column(result, "tokens.would_want_burger", 2)
not_find_column(result, "tokens.would_want")

result = mldb.perform('GET', '/v1/query', [['q', "SELECT tokenize('I would want a burger', {splitchars: ' ', ngram_range: [1, 2]}) as tokens"]])
jsRez = json.loads(result["response"])
mldb.log(jsRez)
find_column(result, "tokens.I_would", 1)

result = mldb.perform('GET', '/v1/query', [['q', "SELECT tokenize('I would want a burger', {splitchars: ' ', ngram_range: {-2, 8}}) as tokens"]])
mldb.log(result)
assert result["statusCode"] == 400

mldb.script.set_return("success")

