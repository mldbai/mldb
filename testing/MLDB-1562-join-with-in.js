/** MLDB-1562-join-with-in.js
    Jeremy Barnes, 7 April 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

*/

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var res = mldb.get('/v1/query', { q: "SELECT _remove_table_name('table1.rowName() IN (KEYS OF { table2.* })', 'table1') NAMED 'res'", format: "table"});

mldb.log(res);

unittest.assertEqual(res.responseCode, 200, "failed to analyze join");

unittest.assertEqual(res.json[1][1], "in(\"function(\"table1\",\"rowName\"),keys,select(columns(\"table2\",\"table2\",[])))");

var res = mldb.get('/v1/query', { q: "SELECT _remove_table_name('table1.rowName() IN (KEYS OF { table2.* })', 'table2') NAMED 'res'", format: "table"});

mldb.log(res);

unittest.assertEqual(res.responseCode, 200, "failed to analyze join");

unittest.assertEqual(res.json[1][1], "in(\"function(\"table1\",\"rowName\"),keys,select(columns(\"table2\",\"table2\",[])))");

// Make sure we get the join type and conditions right
var res = mldb.get('/v1/query', { q: "SELECT _analyze_join('table1', 'table2', 'table1.rowName() IN (KEYS OF ({table2.*}))', 'true') AS *", format: "aos"});

mldb.log(res);

unittest.assertEqual(res.responseCode, 200, "failed to analyze join");

// This is a cross join, at least until we detect a matrix multiply join style
unittest.assertEqual(res.json[0]["style"], "CROSS_JOIN");

// There is no purely left condition
unittest.assertEqual(res.json[0]["left.where"], "constant([1,\"NaD\"])");

// There is no purely right condition
unittest.assertEqual(res.json[0]["right.where"], "constant([1,\"NaD\"])");

// There is a cross join condition
unittest.assertEqual(res.json[0]["crossWhere"], "in(\"function(\"table1\",\"rowName\"),keys,select(columns(\"table2\",\"table2\",[])))");

"success"

