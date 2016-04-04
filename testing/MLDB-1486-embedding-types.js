// This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

function assertEqual(expr, val)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    mldb.log(expr, 'IS NOT EQUAL TO', val);

    throw "Assertion failure";
}


function testQuery(query, expected) {
    mldb.log("testing query", query);

    var resp = mldb.get('/v1/query', {q: query, format: 'table'});

    mldb.log("received", resp.json);
    mldb.log("expected", expected);
    
    assertEqual(resp.responseCode, 200);
    assertEqual(resp.json, expected);
}

var expected = [
   [ "_rowName", "kind", "scalar", "type" ],
   [
      "result",
      "scalar",
      "Datacratic::MLDB::CellValue",
      "Datacratic::MLDB::AtomValueInfo"
   ]
];

testQuery('select static_type(1) as *', expected);

testQuery('select static_known_columns([1,2,3]) as *', expected);

"success"
