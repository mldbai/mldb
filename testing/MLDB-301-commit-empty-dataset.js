// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* MLDB-301 Test commit of empty mutable dataset */

var dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'test',
};

var dataset = mldb.createDataset(dataset_config)

// MLDB-301 will throw here until it is fixed
dataset.commit()

"success"
