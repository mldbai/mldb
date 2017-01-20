#
# timediff_in_seconds.py
# Mich, 2016-04-01
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
# How to get time spans in seconds rather than time strings.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
ds.record_row('row1', [['colA', 1, 1], ['colB', 4, 12]])
ds.commit()

res = mldb.query("""
    SELECT
    CAST(to_timestamp(colB) as integer) - CAST(to_timestamp(colA) as integer)
    FROM ds
""")
mldb.log(res)

assert res[1][1] == 3

mldb.script.set_return("success")
