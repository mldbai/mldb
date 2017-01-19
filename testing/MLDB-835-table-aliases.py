#
# filename
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa


def check(result, expectedSize):
    mldb.log(result.json())
    assert len(result.json()) == expectedSize


def find_value(result, value):
	response = result.json()
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

def check_failed(qry):
    try:
        result = mldb.get('/v1/query', q=qry)
    except mldb_wrapper.ResponseException as exc:
        pass
    else:
        mldb.log(result.json())
        assert False, 'should not be here'

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'x.y'})

ds1.record_row('row1', [['a.b', 7, 0], ['z', 11, 0], ['id', 0, 0]])
ds1.record_row('row2', [['a.b', 5, 0], ['z', 13, 0], ['id', 1, 0]])

ds1.commit()

result = mldb.get('/v1/query', q='SELECT a.b FROM "x.y"')

check(result, 2)

result = mldb.get('/v1/query', q='SELECT "a."* FROM "x.y"')
check(result, 2)

# MLDB-1313 test case
result2 = mldb.get('/v1/query', q='SELECT a.* FROM "x.y"')
assert result.json() == result2.json()

result3 = mldb.query('select id, z from "x.y"')
mldb.log(result3)
result4 = mldb.query('select * excluding(a.*) from "x.y"')
mldb.log(result4)
assert result3 == result4
result5 = mldb.query('select * excluding(a.*) from "x.y"')
assert result3 == result5

result = mldb.get('/v1/query', q='SELECT q.r.a.b FROM "x.y" as "q.r"')

check(result, 2)

result = mldb.get('/v1/query', q='SELECT "q.r".a.b FROM "x.y" as "q.r"')

check(result, 2)

result = mldb.get('/v1/query', q='SELECT "q.r"."a.b" FROM "x.y" as "q.r"')

check(result, 2)

result = mldb.get('/v1/query',
                  q='SELECT "q.r"."a.b" FROM "x.y" as "q.r" ORDER BY "q.r"."a.b"')

check(result, 2)

result = mldb.get(
    '/v1/query',
    q='SELECT "q.r"."a.b" AS "n.m" FROM "x.y" as "q.r" ORDER BY "q.r"."a.b"')

check(result, 2)

result = mldb.get('/v1/query', q='SELECT count(1) FROM "x.y" GROUP BY "a.b"')

check(result, 2)

result = mldb.get('/v1/query',
                  q='SELECT "q.r"."a.b" AS n.m FROM "x.y" as "q.r" GROUP BY "a.b"')

check(result, 2)

result = mldb.get(
    '/v1/query',
    q='SELECT "q.r"."a.b" AS "n.m" FROM "x.y" as "q.r" GROUP BY "a.b"')

check(result, 2)

result = mldb.get(
    '/v1/query',
    q='SELECT "q.r"."a.b" AS "n.m" FROM "x.y" as "q.r" GROUP BY "a.b"')

check(result, 2)

result = mldb.get(
    '/v1/query',
    q='SELECT "q.r"."a.b" AS "n.m" FROM "x.y" as "q.r" GROUP BY "a.b"')

check(result, 2)

result = mldb.get('/v1/query',
                  q='SELECT "a.b" FROM "x.y" as "q.r" GROUP BY "q.r"."a.b"')

check(result, 2)

mldb.log("these should fail");
 
check_failed('SELECT * FROM "x.y" as q.r ORDER BY "x.y"."a.b"')
#check_failed('SELECT count(1) FROM "x.y" as "q.r" GROUP BY "x.y"."a.b"')
#check_failed('SELECT "x.y"."a.b" FROM "x.y" as "q.r"')

mldb.log("Join tests");

ds2 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'x'})

ds2.record_row('row1', [['y.z', 17, 0], ['id', 0, 0]])
ds2.record_row('row2', [['y.z', 19, 0], ['id', 1, 0]])

ds2.commit()

result = mldb.get('/v1/query',
                  q='SELECT * FROM x JOIN "x.y" ON x.id = "x.y".id')

check(result, 2)
find_value(result, 17)

result = mldb.get('/v1/query',
                  q='SELECT "x"."y.z" FROM x JOIN "x.y" ON x.id = "x.y".id')

check(result, 2)
find_value(result, 17)

result = mldb.get('/v1/query',
                  q='SELECT "x.y".z FROM x JOIN "x.y" ON x.id = "x.y".id')

check(result, 2)
find_value(result, 13)

mldb.script.set_return("success")
