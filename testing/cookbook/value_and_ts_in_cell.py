#
# value_and_ts_in_cell.py.py
# Mich, 2016-04-01
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
# How to get "value @ timestamp" as the result of a table query.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
ds.record_row('row1', [['colA', 1, 1], ['colB', 4, 12]])
ds.record_row('row2', [['colA', 2, 132], ['colC', 40, 999]])
ds.commit()

mldb.log(mldb.query("""
    SELECT jseval('
    var res = {};
    for (var i in cols) {
        res[cols[i][0]] = cols[i][1] + " @ " + cols[i][2].toISOString();
    }
    return res;',
    'cols', {*}) AS * FROM ds
"""))

mldb.script.set_return("success")
