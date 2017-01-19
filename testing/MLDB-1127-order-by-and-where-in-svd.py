#
# MLDB-1127-order-by-and-where-in-svd.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

import datetime
import random

mldb = mldb_wrapper.wrap(mldb) # noqa

def train_svd(order_by, where, offset, limit):
    svd_procedure = "/v1/procedures/order_svd"
    # svd procedure configuration
    svd_config = {
        'type' : 'svd.train',
        'params' : {
            "trainingData": {"from" : {"id": "svd_example"},
                             "select" : "x, y, z",
                             "orderBy": order_by,
                             "where": where,
                             "offset" : offset,
                             "limit" : limit
                         },
            "rowOutputDataset": {
                "id": "svd_row",
                'type': "embedding"
            },
            "columnOutputDataset" : {
                "id": "svd_column",
                "type" : "embedding"
            }
        }
    }

    mldb.put(svd_procedure, svd_config)
    mldb.post(svd_procedure + '/runs')

    result = mldb.get('/v1/query', q="SELECT * FROM svd_row")
    return len(result.json()[0]["columns"])


def load_svd_dataset():
    """A dataset with two very different type of rows
    - first 50 rows have 3 independent columns
    - last 50 rows have 2 independent columns
    """

    svd_example = mldb.create_dataset(
        {"type": "sparse.mutable", 'id' : 'svd_example'}
    )
    for i in xrange(0,50):
        val_x = random.randint(1, 1000)
        val_y = random.randint(1, 1000)
        val_z = random.randint(1, 1000)
        # three independent columns
        svd_example.record_row('row_' + str(i), [
            ['x', val_x, now],
            ['y', val_y, now],
            ['z', val_z, now],
            ['index', i, now]
        ])
        # two independent columns
        svd_example.record_row('row_n' + str(i+50), [
            ['x', val_x, now],
            ['y', val_x, now],
            ['z', val_z, now],
            ['index', i+50, now]
        ])
    svd_example.commit()

now = datetime.datetime.now()
load_svd_dataset()
assert train_svd("rowName() ASC", "true", 0, 50) == 3, \
    'expected three independent columns in the first 50 rows'
assert train_svd("rowName() DESC", "true", 0, 50) == 2, \
    'expected two independent columns in the last 50 rows'

assert train_svd("rowName() ASC", "index < 50", 0, 100) == 3, \
    'expected three independent columns in the first 50 rows'
assert train_svd("rowName() ASC", "index > 50", 0, 100) == 2, \
    'expected two independent columns in the last 50 rows'

mldb.script.set_return('success')
