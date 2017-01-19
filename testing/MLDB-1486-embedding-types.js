// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

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
   [ "_rowName", "isConstant", "kind", "scalar", "type" ],
   [
      "result",
      1,
      "scalar",
      "long",
      "MLDB::IntegerValueInfo"
   ]
];

testQuery('select static_type(1) as *', expected);

expected = [
   [
      "_rowName",
      "0.columnName",
      "0.offset",
      "0.sparsity",
      "0.valueInfo.isConstant",
      "0.valueInfo.kind",
      "0.valueInfo.scalar",
      "0.valueInfo.type",
      "1.columnName",
      "1.offset",
      "1.sparsity",
      "1.valueInfo.isConstant",
      "1.valueInfo.kind",
      "1.valueInfo.scalar",
      "1.valueInfo.type",
      "2.columnName",
      "2.offset",
      "2.sparsity",
      "2.valueInfo.isConstant",
      "2.valueInfo.kind",
      "2.valueInfo.scalar",
      "2.valueInfo.type"
   ],
   [
      "result",
      "0",
      0,
      "dense",
      0,
      "scalar",
      "MLDB::CellValue",
      "MLDB::AtomValueInfo",
      "1",
      1,
      "dense",
      0,
      "scalar",
      "MLDB::CellValue",
      "MLDB::AtomValueInfo",
      "2",
      2,
      "dense",
      0,
      "scalar",
      "MLDB::CellValue",
      "MLDB::AtomValueInfo"
   ]
];        

testQuery('select static_known_columns([1,2,3]) as *', expected);

"success"
