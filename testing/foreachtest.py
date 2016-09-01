
mldb = mldb_wrapper.wrap(mldb) # noqa

res = mldb.query("SELECT { { 1 as a, 2 as b} as y, { 3 as a, 4 as b} as z } as x")

mldb.log(res)

res = mldb.query("SELECT ForEach(value(), { { 1 as a, 2 as b} as y, { 3 as a, 4 as b} as z })") 

mldb.log(res)

mldb.script.set_return("success")