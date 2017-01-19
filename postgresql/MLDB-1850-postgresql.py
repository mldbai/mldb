#
# MLDB-1850-postgresql.py
# Mathieu Marquis Bolduc, August 12th 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
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
dataset.record_row("row1", [["x", 1, now], ["y", "alfalfa", now], ["z", 3.5, now]])
dataset.record_row("row2", [["x", 2, now], ["y", "brigade", now], ["z", 5.7, now]])

dataset.commit()

mldb.put("/v1/credentials/postgresqltest", {
  "store":{
    "resource":"localhost",
    "resourceType":"postgresql",
    "credential":{
      "id":"mldb",
      "secret":"mldb",
      "location":"mldb",
      "validUntil":"2030-01-01T00:00:00Z"
    }
  }
})

mldb.log("From MLDB to Postgres")
res = mldb.post('/v1/procedures', {
    'type': 'transform',
    'params': {
        'inputData': 'SELECT y as a, x as b, z as c from input',
        'outputDataset': {
                            'type'    : 'postgresql.recorder',
                            'id'      : 'postgresql',
                            'params': {
                                'createTable' : True,
                                'databaseName' : 'mldb',
                                'port' : 5432,
                                'tableName' : 'mytable',
                                'dropTableIfExist' : True
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

expected = [["_rowName","a","b","c"],
            ["row_0","alfalfa",1,3.5],
            ["row_1","brigade",2,5.7]]

assert res == expected

mldb.log("Query Function")
mldb.put('/v1/functions/query_from_postgres', {
    'type': 'postgresql.query',
    'params': {
        'databaseName' : 'mldb',
        'port' : 5432,
        'query': 'select * from mytable order by a'
    }
})

res = mldb.query("select query_from_postgres()")

mldb.log(res)

expected = [["_rowName","query_from_postgres().a","query_from_postgres().b","query_from_postgres().c"],
            ["result","alfalfa",1,3.5]]

assert res == expected

mldb.log("Postgresql Dataset")
dataset_config = {
    'type'    : 'postgresql.dataset',
    'id'      : 'postgresqldataset',
    'params': {
                'databaseName' : 'mldb',
                'port' : 5432,
                'tableName' : 'mytable',
                'primaryKey' : 'a'
            }
    }

postgresqldataset = mldb.create_dataset(dataset_config)

res = mldb.query("select b from postgresqldataset")

mldb.log(res)

expected = [["_rowName","b"],
            ["brigade",2],
            ["alfalfa",1]]

assert res == expected

res = mldb.query("select * from postgresqldataset")

mldb.log(res)

expected = [["_rowName","a","b","c"],
            ["brigade","brigade",2,5.7],
            ["alfalfa","alfalfa",1,3.5]]

assert res == expected

mldb.script.set_return("success")
