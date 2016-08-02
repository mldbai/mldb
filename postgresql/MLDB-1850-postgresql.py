#
# MLDB-1850-postgresql.py
# Datacratic, 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

# Create mldb dataset
dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'input'
}

dataset = mldb.create_dataset(dataset_config)

now = datetime.datetime.now()
dataset.record_row("row1", [["x", 1, now], ["y", "alfalfa", now]])
dataset.record_row("row2", [["x", 2, now], ["y", "brigade", now]])

dataset.commit()

mldb.log("From MLDB to Postgres")
res = mldb.post('/v1/procedures', {
    'type': 'transform',
    'params': {
        'inputData': 'SELECT y as a, x as b from input',
        'outputDataset': {
                            'type'    : 'postgresql.recorder',
                            'id'      : 'postgresql',
                            'params': {
                                'createTable' : True,
                                'databaseName' : 'mldb',
                                'port' : 5432,
                                'userName' : 'mldb',
                                'tableName' : 'mytable',
                                'createTableColumns' : 'a VARCHAR(32), b integer'
                            }
                        },
        'runOnCreation': True
    }
})

mldb.log(res)

mldb.log("From Postgres to MLDB")

res = mldb.post('/v1/procedures', {
    'type': 'postgresql.import',
    'params': {
        'databaseName' : 'mldb',
        'port' : 5432,
        'userName' : 'mldb',
        'postgresqlQuery' : 'select * from mytable order by a',
        'runOnCreation': True,
        'outputDataset' : {
                    'id' : 'out',
                    'type' : 'sparse.mutable'
                }
    }
})

mldb.log(res)

mldb.log("Re-Query")
res = mldb.query("select * from out")

mldb.log(res)

mldb.log("Query Function")
mldb.put('/v1/functions/query_from_postgres', {
    'type': 'postgresql.query',
    'params': {
        'databaseName' : 'mldb',
        'port' : 5432,
        'userName' : 'mldb',
        'query': 'select * from mytable order by a'
    }
})

res = mldb.query("select query_from_postgres()")

mldb.log(res)

mldb.script.set_return("success")