// Example to demonstrate nearest neighbours search within an embedding
// dataset.

function createDataset()
{
    var dataset_config = {
        'type'    : 'embedding',
        'id'      : 'test',
    };

    var dataset = mldb.createDataset(dataset_config)

    var ts = new Date();

    function recordExample(row, x, y)
    {
        dataset.recordRow(row, [ [ "x", x, ts ], ["y", y, ts] ]);
    }

    recordExample("ex1", 0, 0);
    recordExample("ex2", 0, 1);
    recordExample("ex3", 1, 0);
    recordExample("ex4", 1, 1);

    dataset.commit()

    return dataset;
}

// Create a sample embedding dataset with four points in it
var dataset = createDataset();

// Perform a query for the neighbours of a given set of coordinates
var res1 = mldb.get("/v1/datasets/test/routes/neighbours", {x:0.5,y:0.5}).json;

plugin.log("result of query 1", res1);

// Perform a query for the neighbours of a different set of coordinates
var res2 = mldb.get("/v1/datasets/test/routes/neighbours", {x:0.1,y:0.2}).json;

plugin.log("result of query 2", res2);

// Perform a query for the neighbours of a known row
var res3 = mldb.get("/v1/datasets/test/routes/rowNeighbours", {row: "ex1"}).json;

plugin.log("result of query 3", res3);

// Script has succeeded
"success"
