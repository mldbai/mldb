# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

import json
import datetime

now = datetime.datetime.now()
yesterday = now + datetime.timedelta(days=-1)
tomorrow = now + datetime.timedelta(days=1)

#First create the datasets we'll need
ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dataset'})

for i in xrange(10):
    ds1.record_row('row_' + str(i),
                   [['x', -i, yesterday], ['x', 0, now], ['x', i, tomorrow], ['y', i%2, now]])
ds1.commit()

#temporal min on one column
result = mldb.perform('GET', '/v1/query', [['q', 'select temporal_min(x) as t_min_x from dataset']])

mldb.log(result)

mldb.script.set_return('success')
