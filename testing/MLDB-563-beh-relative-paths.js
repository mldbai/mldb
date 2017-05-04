// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* MLDB-563 can't save behs to relative paths. */

var dataset_config = {
    type: 'beh.mutable',
    id: 'test',
    params: {
        dataFileUrl: "file://tmp/MLDB-563.beh"
    }
};

var dataset = mldb.createDataset(dataset_config)

var ts = new Date("2015-01-01");

function recordExample(row, x, y)
{
    dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts] ]);
}

// Very simple linear regression, with x = y
recordExample("ex1", 0, 0);
recordExample("ex2", 1, 1);
recordExample("ex3", 2, 2);
recordExample("ex4", 3, 3);

plugin.log("committing");

dataset.commit()

"success"
