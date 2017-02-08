// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// See MLDB-238
// Check we can sort a column that has missing values

var datasetConfig = { "id": "ds1", "type": "sparse.mutable" };

var dataset = mldb.createDataset(datasetConfig);

var ts = new Date();

dataset.recordRow("row1", [ [ "Weight", 1, ts ], ["col2", 2, ts] ]);
dataset.recordRow("row2", [ [ "Weight", 2, ts ], ["col3", 2, ts] ]);
dataset.recordRow("row3", [ [ "Weight2", 3, ts ],["col3", 2, ts] ]);

dataset.commit();

var output = mldb.get('/v1/query', { q: 'select * from ds1 order by Weight, col3'});

plugin.log(output);

if (output.json.length != 3)
    throw "Should have returned 3 entries";
if (output.json[0].rowName != "row3")
    throw "Row3 should be first";
if (output.json[1].rowName != "row1")
    throw "Row1 should be second";
if (output.json[2].rowName != "row2")
    throw "Row2 should be third";


"success";
