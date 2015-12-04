/* Example script to import a reddit dataset and run an example */

function createDataset()
{
    var start = new Date();

    var dataset_config = {
        type: 'mutable',
        id: 'reddit_dataset'
    };

    var dataset = mldb.createDataset(dataset_config)

    var dataset_address = 'http://files.figshare.com/1310438/reddit_user_posting_behavior.csv.gz'
    var now = new Date("2015-01-01");

    var stream = mldb.openStream(dataset_address);

    var numLines = 10000;

    var lineNum = 0;
    while (!stream.eof() && lineNum < numLines) {
        ++lineNum;
        if (lineNum % 100000 == 0)
            plugin.log("loaded", lineNum, "lines");
        var line = stream.readLine();
        var fields = line.split(',');
        var tuples = [];
        for (var i = 1;  i < fields.length;  ++i) {
            tuples.push([fields[i], 1, now]);
        }

        dataset.recordRow(fields[0], tuples);
    }

    dataset.commit()

    var end = new Date();
    
    plugin.log("Created Reddit dataset for " + lineNum + " users in "
               + (end - start) / 1000 + " seconds");
    
    return dataset;
}

function succeeded(response)
{
    return response.responseCode >= 200 && response.responseCode < 400;
}

function assertSucceeded(process, response)
{

    if (!succeeded(response)) {
        plugin.log(process, response);
        throw process + " failed: " + JSON.stringify(response);
    }
}

function createAndTrainProcedure(config, name)
{
    var start = new Date();

    var createOutput = mldb.put("/v1/procedures/" + name, config);
    assertSucceeded("procedure " + name + " creation", createOutput);

    // Run the training
    var trainingOutput = mldb.put("/v1/procedures/" + name + "/runs/1", {});
    assertSucceeded("procedure " + name + " training", trainingOutput);

    var end = new Date();

    plugin.log("procedure " + name + " took " + (end - start) / 1000 + " seconds");
}

// 1.  Create our Reddit dataset, by importing actions of 100,000 users.

createDataset();

// 2.  Train our SVD on the Reddit dataset, embedding each of the subreddits
//     in a 100 dimensional space.  We only take the 500 reddits with the
//     highest user counts to limit the runtime

var svdConfig = {
    type: "svd",
    params: {
        dataset: { "id": "reddit_dataset" },
        output: { "id": "reddit_svd_embedding", type: "embedding" },
        select: "COLUMN EXPR (ORDER BY rowCount() DESC, columnName() LIMIT 500)"
    }
};

// 3.  Create a map of the reddits
createAndTrainProcedure(svdConfig, 'reddit_svd');

var tsneConfig = {
    type: "tsne",
    params: {
        dataset: { "id": "reddit_svd_embedding" },
        output: { "id": "reddit_tsne_embedding", "type": "embedding" },
        select: "*",
        where: "true"
    }
};

createAndTrainProcedure(tsneConfig, 'reddit_tsne');

// 4.  Query MLDB to show what the embedding looks like
var output = mldb.get("/v1/datasets/reddit_tsne_embedding/query2",
                      {select: "x, y, rowName() as subreddit", format:'soa'}).json


var plotScript = 
'import bokeh.plotting as bp\n'+
'import numpy as np\n'+
'from bokeh.models import HoverTool \n'+
'from bokeh.resources import CDN\n'+
'from bokeh.embed import autoload_static\n'+
'\n'+
'print mldb.script.args\n'+
'x = mldb.script.args["x"]\n'+
'y = mldb.script.args["y"]\n'+
'subreddit = mldb.script.args["subreddit"]\n'+
'\n'+
'f = bp.figure(plot_width=900, plot_height=700, title="Subreddit Map by t-SNE",\n'+
'              tools="pan,wheel_zoom,box_zoom,reset,hover,previewsave",\n'+
'              x_axis_type=None, y_axis_type=None, min_border=1)\n'+
'f.scatter(\n'+
'    x = x,\n'+
'    y = y,\n'+
'#    color= colormap[clusters[row_selector]], \n'+
'     radius=20, \n'+
'#    radius= np.log2(users_per_subreddit[row_selector])/60, \n'+
'    source=bp.ColumnDataSource({"subreddit": subreddit})\n'+
').select(dict(type=HoverTool)).tooltips = {"/r/":"@subreddit"}\n'+
'\n'+
'\n'+
'script, tag = autoload_static(f, CDN, "procedures/reddit_svd_tsne_output.js")\n'+
'\n'+
'with open("tag.html", "w") as file:\n'+
'    file.write(tag)\n'+
'with open("script.js", "w") as file:\n'+
'    file.write(script)\n'+
'mldb.script.set_return({"script":script, "tag":tag})\n';

var plotConfig = {
    source: plotScript,
    args: output
};

var resp = mldb.post("/v1/types/plugins/python/routes/run", plotConfig);

plugin.log(resp.json['return']);


// Let the script runner know that the script succeeded
"success"
