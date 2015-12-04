// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

plugin.log('getting dataset');

var dataset = mldb.createDataset({id: 'medicx', type: 'beh', address: '/home/mailletf/workspace/mldb/mldb_data/medicx.beh.gz'});

plugin.log(mldb.get('/v1/datasets/medicx').json);

plugin.log('querying');

plugin.log(mldb.get('/v1/datasets/medicx/query', {limit:5}).json);
