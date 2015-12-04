# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import datetime
import json

dataset_config = { "id": "test_data", "type": "sparse.mutable" };
dataset = mldb.create_dataset(dataset_config);
now = datetime.datetime.now()

for index in range(4):
    dataset.record_row("row" + str(index), [ ["index", index, now ] ]);

dataset.commit();

def order_by(order_by):
    output = mldb.perform("GET", "/v1/query", [['q', 'select * from test_data order by ' + order_by]])['response']
    mldb.log(output)
    return [ row['columns'][0][1] for row in json.loads(output)]

def assert_equal(received, expected):
    assert received == expected, 'invalid row order: got %s, expected %s' % (received, expected)

def compare(left, right, op = '<'):
    result = mldb.perform('GET', '/v1/query', [['q', "select tokenize('" + 
                                                left + "', {splitchars:' '}) " + op + " tokenize('" 
                                                + right + "', {splitchars:' '}) as comp"]])
    response = json.loads(result['response'])
    mldb.log(response)
    return response[0]['columns'][0][1]

    
mldb.log('testing ordering of row type with sorted columns...')
order =  order_by('case when index % 4 = 1 then {a:1, b:2} when index % 4 = 2 then {a:0, b:3} when index % 4 = 3 then {a:0, c:4} else null end')
assert_equal(order, [0, 2, 3, 1])

mldb.log('testing ordering of row type with unsorted columns...')
order = order_by('case when index % 4 = 1 then {a:1, b:2} when index % 4 = 2 then {b:3, a:0} when index % 4 = 3 then {c:4, a:0} else null end') 
assert_equal(order, [0, 2, 3, 1])

mldb.log('testing ordering of different types...')
order = order_by('case when index % 4 = 1 then 3 when index % 4 = 2 then null when index % 4 = 3 then {a:1, b:2} else [0,3] end') 
assert_equal(order, [2, 1, 3, 0])

mldb.log('testing ordering of embedding types...')
order = order_by('case when index % 4 = 1 then [3.3, 34.0] when index % 4 = 2 then [64.2, 34.0] when index % 4 = 3 then [64.1999, 34.0] else [3.3,3.0] end') 
assert_equal(order, [0, 1, 3, 2])

smaller = compare('string string zoo', 'string zoo')
assert not smaller, 'left string is greater than right string'

smaller = compare('string zoo','string zoo')
assert not smaller, 'left string is equal to right string'

smaller = compare('zoo string string','string zoo')
assert not smaller, 'left string is greater than right string'

equal = compare('zoo string string','string zoo string','=')
assert equal, 'left string is equal to right string'

equal = compare('zoo string str','zoo string string','=')
assert not equal, 'left string is not equal to right string'

greater = compare('zoo string string','string zoo string','>')
assert not greater, 'left string is not greater than right string'

greater = compare('zoo string string string','zoo string string','>')
assert greater, 'left string is greater than right string'

str_dataset_config = { "id": "str_test_data", "type": "sparse.mutable" };
str_dataset = mldb.create_dataset(str_dataset_config);

str_dataset.record_row("row1", [ ["terms", "c++,python,c++,java,c++", now] ])
str_dataset.record_row("row2", [ ["terms", "scala,scala,java,java,scala,java,scala,c++", now] ])
str_dataset.record_row("row3", [ ["terms", "python,ada,ada", now ] ]);
str_dataset.commit();

output = mldb.perform("GET", "/v1/query", 
                      [['q', "select tokenize(terms) as term from str_test_data order by tokenize(terms)"]])['response']
mldb.log(json.loads(output))
assert_equal([row['rowName'] for row in json.loads(output)], ['row3', 'row2', 'row1'])


mldb.script.set_return('success')
