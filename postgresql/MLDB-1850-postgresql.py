#
# MLDB-1850-postgresql.py
# Datacratic, 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

dataset_config = {
    'type'    : 'postgresql.dataset',
    'id'      : 'postgresql'
}

dataset = mldb.create_dataset(dataset_config)

mldb.script.set_return("success")