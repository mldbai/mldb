#
# MLDB-619_newlines_in_sql.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

dataset = mldb.create_dataset({"type": "sparse.mutable", "id": "x" })
dataset.record_row("rowname", [["colname", 0, 0]])
dataset.commit()

res = [
    mldb.get("/v1/query", q="select * from x"),
    mldb.get("/v1/query", q="select *\nfrom x")
    ]

broken = False
for r in res:
    mldb.log(r.text)

mldb.script.set_return("success")
