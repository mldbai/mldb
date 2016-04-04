#
# timediff_in_seconds.py
# Mich, 2016-04-01
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
# How to get time spans in seconds rather than time strings.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
ds.record_row('row1', [['colA', 1, 1], ['colB', 4, 12]])
ds.commit()

res = mldb.query("""
    SELECT jseval('
    mldb.log(cols);
    mldb.log(cols[0][1]);
    mldb.log(cols[0][1] - cols[1][1]);
    var res = {};
    return {"res" : (cols[0][1] - cols[1][1]) / 1000};',
    'cols', {to_timestamp(colB), to_timestamp(colA)}) AS * FROM ds
""")
mldb.log(res)

assert res[1][1] == 3

mldb.script.set_return("success")
