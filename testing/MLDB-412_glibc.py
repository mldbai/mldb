#
# MLDB-412_glibc.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa


dataset = mldb.create_dataset({
    "type": "sparse.mutable",
    "id": "x"
})

dataset.record_row("a", [["b",1,1]])
dataset.record_row("b", [["b",1,1]])
dataset.record_row("c", [["b",1,1]])
dataset.record_row("d", [["b",1,1]])
dataset.record_row("e", [["b",1,1]])
dataset.record_row("f", [["b",1,1]])
dataset.record_row("g", [["b",1,1]])
dataset.commit()



mldb.get("/v1/query", q='select max("b") from x group by \'1\'')
mldb.script.set_return("success")
