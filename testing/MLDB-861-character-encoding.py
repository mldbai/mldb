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

csv_conf = {
    "type": "import.text",
    "params": {
        'dataFileUrl' : "file://mldb/testing/utf8.csv",
        "ouputDataset": {
            "id": "utf8",
        },
        "runOnCreation" : True,
        "encoding": 'utf8',
    }
}
mldb.put("/v1/procedures/csv_proc", csv_conf) 

csv_conf = {
    "type": "import.text",
    "params": {
        'dataFileUrl' : "file://mldb/testing/latin1.csv",
        "ouputDataset": {
            "id": "latin1",
        },
        "runOnCreation" : True,
        "encoding": 'latin1',
    }
}
mldb.put("/v1/procedures/csv_proc", csv_conf) 

result = mldb.get("/v1/query", q="select * from utf8")
mldb.log(result)

result = mldb.get("/v1/query", q="select * from latin1")
mldb.log(result)

mldb.script.set_return("success")
