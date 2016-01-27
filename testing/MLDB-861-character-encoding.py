#
# MLDB-861-character-encoding.py
# datacratic, 2015
# this file is part of mldb. copyright 2015 datacratic. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

"""
there are two files with the same contents, differently encoded
mldb/testing/utf8.csv and mldb/testing/latin1.csv
contents:
Age,Nâme
12,Niçolâß
"""

result = mldb.post("/v1/datasets", {
    "type": 'text.csv.tabular',
    "id": 'utf8',
    "params": {
        "dataFileUrl": 'file://mldb/testing/utf8.csv',
        "encoding": 'utf8'
    }
})
mldb.log(result)

result = mldb.post("/v1/datasets", {
    "type": 'text.csv.tabular',
    "id": 'latin1',
    "params": {
        "dataFileUrl": 'file://mldb/testing/latin1.csv',
        "encoding": 'latin1'
    }
})
mldb.log(result)

result = mldb.get("/v1/query", q="select * from utf8")
mldb.log(result)

result = mldb.get("/v1/query", q="select * from latin1")
mldb.log(result)

mldb.script.set_return("success")
