# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json
import datetime

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'example'
}

dataset = mldb.create_dataset(dataset_config)

d = datetime.datetime.now()

dataset.record_row('row1', [ [ "x", "2015-01-01T15:14:39.123456Z", d ] ])

mldb.log("Committing dataset")
dataset.commit()

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'example2'
}

dataset2 = mldb.create_dataset(dataset_config)
d = datetime.datetime.now()
dataset2.record_row('row1', [ [ "x", "2014-12-31T15:14:39.123456Z", d ] ])
mldb.log("Committing dataset")
dataset2.commit()


result = mldb.perform('GET', '/v1/query', [['q', 'select * from example']])
mldb.log(result)

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('year', when(x)) as year from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == d.year

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('month', when(x)) as month from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == d.month

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('quarter', when(x)) as quarter from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == (d.month / 4) + 1

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('day', when(x)) as day from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == d.day

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('hour', when(x)) as hour from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == d.hour

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('minute', when(x)) as minute from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == d.minute

#these ones use X instead of when(x) because its not directly in date_time

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('second', x) as second from example"]])
mldb.log(result)
mldb.log(d.second)
assert json.loads(result['response'])[0]['columns'][0][1] == 39

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('millisecond', x) as millisecond from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 123

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('microsecond', x) as microsecond from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 123456

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('dow', x) as dow from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 4

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('doy', x) as doy from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 0 #"days since january 1st"

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('isodow', x) as isodow from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 4 

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('isodoy', x) as isodoy from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 4 

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('week', x) as week from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 0

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('isoweek', x) as isoweek from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 1

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('isoyear', x) as isoyear from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 2015

#try with a december date

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('second', x) as second from example2"]])
mldb.log(result)
mldb.log(d.second)
assert json.loads(result['response'])[0]['columns'][0][1] == 39

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('millisecond', x) as millisecond from example2"]])
mldb.log(result)
assert 122 <= json.loads(result['response'])[0]['columns'][0][1] <= 124

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('microsecond', x) as microsecond from example2"]])
mldb.log(result)
assert 123455 <= json.loads(result['response'])[0]['columns'][0][1] <= 123457

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('dow', x) as dow from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 3

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('doy', x) as doy from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 364 #"days since january 1st"

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('isodow', x) as isodow from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 3 

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('isodoy', x) as isodoy from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 3

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('week', x) as week from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 52

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('isoweek', x) as isoweek from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 1

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('isoyear', x) as isoyear from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 2015

#previous year
dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'example3'
}

dataset3 = mldb.create_dataset(dataset_config)
d = datetime.datetime.now()
dataset3.record_row('row1', [ [ "x", "2014-12-28T15:14:39.123456Z", d ] ])
mldb.log("Committing dataset")
dataset3.commit()

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('isoweek', x) as isoweek from example3"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 52

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('isodoy', x) as isodoy from example3"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 364


##test for date_trunc 

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('second', x) as second from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts']  == "2014-12-31T15:14:39Z"

#problematic because floating points
result = mldb.perform('GET', '/v1/query', [['q', "select date_part('millisecond',date_trunc('millisecond', x)) as millisecond from example2"]])
mldb.log(result)
assert 122 <= json.loads(result['response'])[0]['columns'][0][1] <= 124
result = mldb.perform('GET', '/v1/query', [['q', "select date_part('microsecond',date_trunc('millisecond', x)) as millisecond from example2"]])
mldb.log(result)
assert 122999 <= json.loads(result['response'])[0]['columns'][0][1] <= 123001

#problematic because floating points
result = mldb.perform('GET', '/v1/query', [['q', "select date_part('microsecond',date_trunc('microsecond', x)) as microsecond from example2"]])
mldb.log(result)
assert 123455 <= json.loads(result['response'])[0]['columns'][0][1] <= 123457

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('minute', x) as minute from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-31T15:14:00Z"

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('hour', x) as hour from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-31T15:00:00Z"

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('day', x) as day from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-31T00:00:00Z"

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('month', x) as month from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-01T00:00:00Z"

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('quarter', x) as quarter from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-09-01T00:00:00Z"

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('year', x) as year from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-01-01T00:00:00Z"

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('dow', x) as dow from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-31T00:00:00Z"

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('doy', x) as doy from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-31T00:00:00Z"

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('isodow', x) as isodow from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-31T00:00:00Z"

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('isodoy', x) as isodoy from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-31T00:00:00Z"

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('week', x) as week from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-28T00:00:00Z" # previous sunday

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('isoweek', x) as isoweek from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-29T00:00:00Z" # previous monday

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('isoyear', x) as isoyear from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-29T00:00:00Z" # first monday of the first iso week

#MLDB-990

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('hour', x, '+0100') as hour from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 16

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('hour', x, '-01') as hour from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 14

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('hour', x, '+05:50') as hour from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 21


result = mldb.perform('GET', '/v1/query', [['q', "select date_part('hour', x, '+12:00') as hour from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 3

result = mldb.perform('GET', '/v1/query', [['q', "select date_part('day', x, '+12:00') as hour from example"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1] == 2

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('minute', x, '+00:30') as minute from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-31T15:44:00Z"

result = mldb.perform('GET', '/v1/query', [['q', "select date_trunc('hour', x, '-08:00') as hour from example2"]])
mldb.log(result)
assert json.loads(result['response'])[0]['columns'][0][1]['ts'] == "2014-12-31T07:00:00Z"

###

mldb.script.set_return("success")
