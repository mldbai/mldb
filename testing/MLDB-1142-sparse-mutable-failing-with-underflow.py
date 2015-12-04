# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json
import datetime
import requests
import random

def load_kmeans_dataset():
    """A dataset with two 'time slices' 
    - a slice with two clear clusters along the x axis _now_ timestamp and
    - a slice with two clear clusters along the y axis with tomorrow's timestamp
    This will serve at testing if the when clause was applied correctly"""

    kmeans_example = mldb.create_dataset({"type": "sparse.mutable", 'id' : 'kmeans_example'})
    for j in range(0,10):
        val_x = float(random.randint(-5, 5))
        val_y = float(random.randint(-5, 5))
        row = [
            ['x', val_x, now], ['y', val_y, now]
        ]
        kmeans_example.record_row('row_%d' % j, row)
        mldb.log("x %f, y %f" % (val_x, val_y))

    kmeans_example.commit()

    response = mldb.perform('GET', '/v1/query', [['q','select * from kmeans_example']])
    mldb.log(response['response'])
    mldb.log(json.loads(response['response']))


now = datetime.datetime.now()

load_kmeans_dataset()


mldb.script.set_return('success')
