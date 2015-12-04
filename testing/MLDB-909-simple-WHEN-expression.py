# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json
import datetime

now = datetime.datetime.now()
a_second_before_now = now + datetime.timedelta(seconds=-1)
same_time_tomorrow = now + datetime.timedelta(days=1)
in_two_hours = now + datetime.timedelta(hours=2)


ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dataset1'})

row_count = 10
for i in xrange(row_count - 1):
    # row name is x's value
    ds1.record_row(str(i), [['x', i, now]])
ds1.record_row(str(row_count - 1), [['x', 9, same_time_tomorrow]])
ds1.commit()

# getting all tuples
result = mldb.perform('GET', '/v1/query', [['q', "SELECT * FROM dataset1"]])
assert result['statusCode'] == 200
rows = json.loads(result["response"])
for row in rows:
    assert row["rowName"] == row["columns"][0][1], 'expected tuple matching row name %s' % row["rowName"]

result = mldb.perform('GET', '/v1/query', [['q', "SELECT * FROM dataset1 WHEN timestamp() BETWEEN '2015-01-01' AND '2030-01-06'"]])
assert result['statusCode'] == 200
rows = json.loads(result["response"])
for row in rows:
    assert row["rowName"] == row["columns"][0][1], 'expected tuple matching row name %s' % row["rowName"]

# getting no tuples
result = mldb.perform('GET', '/v1/query', [['q', "SELECT * FROM dataset1 WHEN timestamp() BETWEEN '2015-01-01' AND '2015-06-06'"]])
assert result['statusCode'] == 200
rows = json.loads(result["response"])
mldb.log(rows)
for row in rows:
    assert "columns" not in row, 'no tuple should be returned on row name %s' % row["rowName"]

# check that where get one row
result = mldb.perform('GET', '/v1/query', [['q', "SELECT x FROM dataset1 WHERE x = 9"]])
assert result['statusCode'] == 200
rows = json.loads(result["response"])
assert len(rows) == 1, "expecting only one row to be selected by the where clause"
assert rows[0]["columns"][0][1] == 9, "expecting row 9 to be selected by where clause"

# check that the last tuple is filtered out
result = mldb.perform('GET', '/v1/query', 
                      [['q', "SELECT x FROM dataset1 WHEN timestamp() between '%s' and '%s'" % (a_second_before_now, in_two_hours)]])
assert result['statusCode'] == 200
rows = json.loads(result["response"])
mldb.log(rows)
for row in rows:
    if row['rowName'] is 9:
        assert row["columns"][0][1] is None, "expecting row 9 to be filtered out by the when clause"

# check that the when clause is executed after the where one
result = mldb.perform('GET', '/v1/query', 
                      [['q', "SELECT x FROM dataset1 WHEN timestamp() between '%s' and '%s' WHERE x = 9" % (a_second_before_now, in_two_hours)]])
assert result['statusCode'] == 200
rows = json.loads(result["response"])
assert rows[0]["columns"][0][1] is None and len(rows) == 1, "expecting the tuple to be filtered out by when clause"
assert rows[0]['rowName'] is 9, "expecting only row 9 to remain"

mldb.script.set_return('success')
