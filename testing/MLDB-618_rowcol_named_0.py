#
# MLDB-618_rowcol_named_0.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
import random

mldb = mldb_wrapper.wrap(mldb) # noqa

dataset = mldb.create_dataset({ "type": "sparse.mutable", "id": "x" })

try:
    res = dataset.record_row(0, [])
except:
    assert False, "No exception expected"
else:
    pass

for r  in range(100):
    dataset.record_row( r, [ [c, random.random(), 0] for c in range(100)] )
for r in range(100, 200):
    dataset.record_row( str(r), [ [c, random.random(), 0] for c in range(100)] )
dataset.commit()

res = mldb.get("/v1/query", q="select * from x")
mldb.script.set_return("success")
