#
# MLDB-558-python-unicode.py
# datacratic, 2015
# this file is part of mldb. copyright 2015 datacratic. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

mldb.create_dataset({"id": "hellô", "type":"embedding"})

result = mldb.get("/v1/query", q=unicode("select * from \"hellô\"",
                                         encoding='utf-8'))
mldb.log(result.text)

result = mldb.get("/v1/datasets")
mldb.log(result.text)

result = mldb.put("/v1/datasets/hôwdy", {"type": 'embedding'})
mldb.log(result.text)

result = mldb.get("/v1/datasets")
mldb.log(result.text)

result = mldb.post("/v1/datasets", {
    "type": 'embedding',
    "id": "hî"
})
mldb.log(result.text)

result = mldb.get("/v1/datasets")
mldb.log(result.text)

mldb.script.set_return("success")
