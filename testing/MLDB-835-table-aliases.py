# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json

def Check(result, expectedSize):
    assert(result['statusCode'] == 200)
    response = json.loads(result['response'])
    assert len(response) == expectedSize

def CheckFailed(result):
	assert(result['statusCode'] >= 400)

def FindValue(result, value):
	assert(result['statusCode'] == 200)
	response = json.loads(result['response'])
	numRow = len(response)
	found = False
	for rowIndex in range (0, numRow):
		row = response[rowIndex]['columns']
		numCol = len(row)
		for colIndex in range (0, numCol):
			if row[colIndex][1] == value:
				found = True
				break
		if (found):
			break
	assert found

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'x.y'})

ds1.record_row('row1', [['a.b', 7, 0], ['z', 11, 0], ['id', 0, 0]])
ds1.record_row('row2', [['a.b', 5, 0], ['z', 13, 0], ['id', 1, 0]])

ds1.commit()

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT a.b FROM x.y']])

Check(result, 2)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT "a."* FROM x.y']])

Check(result, 2)

# uncomment for MLDB-1313 test case
#result = mldb.perform('GET', '/v1/query', [['q', 'SELECT a.* FROM x.y']])
#Check(result, 2)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT q.r.a.b FROM x.y as q.r']])

Check(result, 2)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT "q.r".a.b FROM x.y as q.r']])

Check(result, 2)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT "q.r"."a.b" FROM x.y as q.r']])

Check(result, 2)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT "q.r"."a.b" FROM x.y as q.r ORDER BY "q.r"."a.b"']])

Check(result, 2)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT "q.r"."a.b" AS "n.m" FROM x.y as q.r ORDER BY "q.r"."a.b"']])

Check(result, 2)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT count(1) FROM x.y GROUP BY a.b']])

Check(result, 2)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT q.r.a.b AS n.m FROM x.y as q.r GROUP BY a.b']])

Check(result, 2)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT "q.r"."a.b" AS "n.m" FROM "x.y" as "q.r" GROUP BY a.b']])

Check(result, 2)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT "q.r".a.b AS "n.m" FROM "x.y" as "q.r" GROUP BY "a.b"']])

Check(result, 2)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT "q.r"."a.b" AS "n.m" FROM "x.y" as "q.r" GROUP BY "a.b"']])

Check(result, 2)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT a.b FROM x.y as q.r GROUP BY q.r.a.b']])

Check(result, 2)

mldb.log("these should fail");

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT * FROM "x.y" as q.r ORDER BY "x.y"."a.b"']])

CheckFailed(result)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT count(1) FROM "x.y" as "q.r" GROUP BY "x.y"."a.b"']])

CheckFailed(result)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT "x.y"."a.b" FROM "x.y" as "q.r"']])

CheckFailed(result)

mldb.log("Join tests");

ds2 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'x'})

ds2.record_row('row1', [['y.z', 17, 0], ['id', 0, 0]])
ds2.record_row('row2', [['y.z', 19, 0], ['id', 1, 0]])

ds2.commit()

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT * FROM x JOIN x.y ON x.id = "x.y".id']])

Check(result, 2)
FindValue(result, 17)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT x.y.z FROM x JOIN x.y ON x.id = "x.y".id']])

Check(result, 2)
FindValue(result, 17)

result = mldb.perform('GET', '/v1/query', [['q', 'SELECT "x.y".z FROM x JOIN x.y ON x.id = "x.y".id']])

Check(result, 2)
FindValue(result, 13)

mldb.script.set_return("success")
