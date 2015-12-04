# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json, datetime

dataset = mldb.create_dataset({
        'type': 'sparse.mutable',
        'id': 'example_large'})

for i in xrange(20000):
    dataset.record_row("u%d" % i, [['x', "whatever", 0]])

dataset.commit();

expected = [
   [ "_rowName", "x" ],
   [ "u1", "whatever" ],
   [ "u12345", "whatever"],
   [ "u12", "whatever" ],
   [ "u123", "whatever" ],
   [ "u1234", "whatever" ]
];

now = datetime.datetime.now()

result = mldb.perform('GET', '/v1/query', [['q', "select * from example_large WHERE rowName() IN ('u1', 'u12', 'u123', 'u1234', 'u12345', 'u123456')"], ['format', 'table']])
mldb.log(result)

delta = datetime.datetime.now() - now;

mldb.log(delta.seconds)
mldb.log(delta.microseconds)

assert delta.microseconds < 15000 # should take ~1k us with optim, +20k without

assert json.loads(result['response']) == expected

mldb.script.set_return("success")
