#
# get_all_column_names.py
# Mich, 2016-03-29
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
# How to get all column names from a datasets.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
ds.record_row('user1', [['eventA', 1, 0]])
ds.record_row('user2', [['eventB', 1, 0]])
ds.commit()

mldb.log(mldb.get('/v1/datasets/ds/columns').json())

mldb.script.set_return("success")
