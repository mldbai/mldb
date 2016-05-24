#
# MLDB-1594-aggregator-empty-row.py
# datacratic, 2016
# this file is part of mldb. copyright 2016 datacratic. all rights reserved.
#
if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa

res1 = mldb.query("select {}")

res2 = mldb.query("select sum({*}) named 'result' from (select {})")

assert res1 == res2

mldb.script.set_return("success")