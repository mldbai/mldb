// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

/* Example script to import a reddit dataset and run an example */

function createDataset(style, iface)
{
    var start = new Date();

    var dataset_config = {
        type: 'sparse.mutable',
        id: 'reddit_dataset_' + style + "_" + iface
    };

    var dataset = mldb.createDataset(dataset_config)

    var dataset_address = 'https://public.mldb.ai/reddit.csv.gz'
    var now = new Date("2015-01-01");

    var stream = mldb.openStream(dataset_address);

    var numLines = 1000;

    var lineNum = 0;

    function recordRowFunc(rowName, cols)
    {
        dataset.recordRow(rowName, cols);
    }

    function recordRowRest(rowName, cols)
    {
        mldb.post("/v1/datasets/reddit_dataset_" + style + "_" + iface + "/rows",
                  {rowName: rowName, columns: cols});
    }

    function recordRowsFunc(rows)
    {
        dataset.recordRows(rows);
    }

    function recordRowsRest(rows)
    {
        mldb.post("/v1/datasets/reddit_dataset_" + style + "_" + iface + "/multirows",
                  rows);
    }

    var recordRow = iface == "rest" ? recordRowRest : recordRowFunc;
    var recordRows = iface == "rest" ? recordRowsRest : recordRowsFunc;

    function recordColumnFunc(columnName, rows)
    {
        dataset.recordColumn(columnName, rows);
    }

    function recordColumnRest(columnName, rows)
    {
        mldb.post("/v1/datasets/reddit_dataset_" + style + "_" + iface + "/columns",
                  {columnName: columnName, rows: rows});
    }

    function recordColumnsFunc(columns)
    {
        dataset.recordColumns(columns);
    }

    function recordColumnsRest(columns)
    {
        mldb.post("/v1/datasets/reddit_dataset_" + style + "_" + iface + "/multicolumns",
                  columns);
    }

    var recordColumn = iface == "rest" ? recordColumnRest : recordColumnFunc;
    var recordColumns = iface == "rest" ? recordColumnsRest : recordColumnsFunc;

    if (style == "row") {
        // One row at a time
        while (!stream.eof() && lineNum < numLines) {
            ++lineNum;
            if (lineNum % 1000 == 0)
                plugin.log("loaded", lineNum, "lines");
            var line = stream.readLine();
            var fields = line.split(',');
            var tuples = [];
            for (var i = 1;  i < fields.length;  ++i) {
                tuples.push([fields[i], 1, now]);
            }

            recordRow(fields[0], tuples);
        }
    }
    else if (style == "rows") {
        // Multiple rows at a time
        var rows = [];

        while (!stream.eof() && lineNum < numLines) {
            ++lineNum;
            if (lineNum % 1000 == 0)
                plugin.log("loaded", lineNum, "lines");
            var line = stream.readLine();
            var fields = line.split(',');
            var tuples = [];
            for (var i = 1;  i < fields.length;  ++i) {
                tuples.push([fields[i], 1, now]);
            }

            rows.push([fields[0], tuples]);

            // Commit every 5,000
            if (rows.length >= 5000) {
                recordRows(rows);
                rows = [];
            }
        }

        recordRows(rows);
    }
    else if (style == "col") {
        // One column at a time
        while (!stream.eof() && lineNum < numLines) {
            ++lineNum;
            if (lineNum % 1000 == 0)
                plugin.log("loaded", lineNum, "lines");
            var line = stream.readLine();
            var fields = line.split(',');
            var tuples = [];
            for (var i = 1;  i < fields.length;  ++i) {
                tuples.push([fields[i], 1, now]);
            }

            recordColumn(fields[0], tuples);
        }

    }
    else if (style == "cols") {
        // Multiple rows at a time
        var columns = [];

        while (!stream.eof() && lineNum < numLines) {
            ++lineNum;
            if (lineNum % 1000 == 0)
                plugin.log("loaded", lineNum, "lines");
            var line = stream.readLine();
            var fields = line.split(',');
            var tuples = [];
            for (var i = 1;  i < fields.length;  ++i) {
                tuples.push([fields[i], 1, now]);
            }

            columns.push([fields[0], tuples]);

            // Commit every 5,000
            if (columns.length >= 5000) {
                recordColumns(columns);
                columns = [];
            }
        }

        recordColumns(columns);
    }

    plugin.log("Committing dataset")
    dataset.commit()

    var end = new Date();
    
    plugin.log("creating dataset style " + style + " iface " + iface + " took "
               + (end - start) / 1000 + " seconds");
    
    return dataset;
}

var row = createDataset('row', 'func');
var rows = createDataset('rows', 'func');
var col = createDataset('col', 'func');
var cols = createDataset('cols', 'func');

var col_tr = mldb.createDataset({type: 'transposed', id: 'reddit_dataset_col_tr_func', params: { dataset: { id: 'reddit_dataset_col_func' } } });

var cols_tr = mldb.createDataset({type: 'transposed', id: 'reddit_dataset_cols_tr_func', params: { dataset: { id: 'reddit_dataset_cols_func' } } });

var res1 = mldb.get("/v1/query", {q: "select * from reddit_dataset_row_func order by rowHash() limit 10", format:"sparse"}).json;
var res2 = mldb.get("/v1/query", {q: "select * from reddit_dataset_rows_func order by rowHash() limit 10", format:"sparse"}).json;
var res3 = mldb.get("/v1/query", {q: "select * from reddit_dataset_col_tr_func order by rowHash() limit 10", format:"sparse"}).json;
var res4 = mldb.get("/v1/query", {q: "select * from reddit_dataset_cols_tr_func order by rowHash() limit 10", format:"sparse"}).json;

var res5 = mldb.get("/v1/query", {q: "select * from reddit_dataset_col_func order by rowHash() limit 10", format:"sparse"}).json;
var res6 = mldb.get("/v1/query", {q: "select * from reddit_dataset_cols_func order by rowHash() limit 10", format:"sparse"}).json;




//plugin.log(res1);
//plugin.log(res2);

plugin.log("test1");
assertEqual(res1, res2);
plugin.log("test2");
assertEqual(res1, res3);
plugin.log("test3");
assertEqual(res1, res4);

plugin.log("test4");

assertEqual(res5, res6);


var row_rest = createDataset('row', 'rest');
var rows_rest = createDataset('rows', 'rest');
var col_rest = createDataset('col', 'rest');
var cols_rest = createDataset('cols', 'rest');

plugin.log(mldb.get("/v1/datasets").json);

var res7 = mldb.get("/v1/query", {q: "select * from reddit_dataset_row_rest order by rowHash() limit 10", format:"sparse"}).json;
var res8 = mldb.get("/v1/query", {q: "select * from reddit_dataset_rows_rest order by rowHash() limit 10", format:"sparse"}).json;

plugin.log("test5");

assertEqual(res1, res7);
plugin.log("test6");
assertEqual(res1, res8);


var res9 = mldb.get("/v1/query", {q: "select * from reddit_dataset_col_rest order by rowHash() limit 10", format:"sparse"}).json;
var res10 = mldb.get("/v1/query", {q: "select * from reddit_dataset_cols_rest order by rowHash() limit 10", format:"sparse"}).json;

plugin.log("test7");

assertEqual(res5, res9);

plugin.log("test8");

assertEqual(res5, res10);


"success"
