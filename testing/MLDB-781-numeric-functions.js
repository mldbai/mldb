// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

var test_data = mldb.createDataset({type:'sparse.mutable',id:'test'});

var ts = new Date("2015-01-01");

function recordExample(dataset, row, x)
{
    dataset.recordRow(row, [ [ "value", x, ts ] ]);
}

recordExample(test_data, "zero", 0);
recordExample(test_data, "negative", -1);
recordExample(test_data, "positive", 1);
recordExample(test_data, "four", 4);
recordExample(test_data, "float1", "12.4343454");
recordExample(test_data, "float2", "-12.4343454");
//recordExample(test_data, "float3", "9.99999999999999999999999999999");
//recordExample(test_data, "float4", "0.00000000000000000000000000001");


test_data.commit()

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

function assertContains(str, val, msg)
{
    if (str.indexOf(val) != -1)
        return;

    plugin.log("expected", val);
    plugin.log("received", str);

    throw "Assertion failure: " + msg + ": string '"
        + str + "' does not contain '" + val + "'";
}

function callAndAssert(func, row, expected, message)
{
    var resp = mldb.get("/v1/query", {q: "SELECT " + func + " FROM test WHERE rowName()='"+row+"'"});
    assertEqual(resp.responseCode, 200, "GET failed");
    assertEqual(resp.json[0].columns[0][1], expected, func + "   " +  message);
}

/* abs */
callAndAssert("abs(1)", "positive", 1, "failed on positive number");
callAndAssert("abs(0)", "zero", 0, "failed on zero");
callAndAssert("abs(-1)", "negative", 1, "failed on negative number");

/* power */
callAndAssert("power(1,2)", "positive", 1, "failed on positive number");
callAndAssert("power(0,2)", "zero", 0, "failed on zero");
callAndAssert("power(4,2)", "four", 16, "failed on four");
callAndAssert("power(-1,2)", "negative", 1, "failed on negative number");

/* sqrt */
callAndAssert("sqrt(1)", "positive", 1, "failed on positive number");
callAndAssert("sqrt(0)", "zero", 0, "failed on zero");
callAndAssert("sqrt(4)", "four", 2, "failed on four");

/* square root of negative number should be failing */
var resp = mldb.get("/v1/query", {q:"SELECT sqrt(value) from test where rowName()='negative'"});
assertEqual(resp.responseCode, 400, "expecting 400 on invalid input");

/* sqrt undo power and vice versa */
callAndAssert("sqrt(power(1, 2))", "positive", 1, "failed on positive number");
callAndAssert("sqrt(power(0, 2))", "zero", 0, "failed on zero");
callAndAssert("sqrt(power(4,2))", "four", 4, "failed on four");

/* modulus */
callAndAssert("mod(1, 2)", "positive", 1, "failed on positive number");
callAndAssert("mod(0, 2)", "zero", 0, "failed on zero");
callAndAssert("mod(4,2)", "four", 0, "failed on four");
callAndAssert("mod(-1,2)", "negative", -1, "mod failed on -1");

/* ceiling */
callAndAssert("ceil(1)", "positive", 1, "failed on positive number");
callAndAssert("ceil(0)", "zero", 0, "failed on zero");
callAndAssert("ceil(12.4343454)", "float1", 13, "failed on 12.4343454");
callAndAssert("ceil(-12.4343454)", "float2", -12, "failed on -12.4343454");

/* floor */
callAndAssert("floor(1)", "positive", 1, "failed on positive number");
callAndAssert("floor(0)", "zero", 0, "failed on zero");
callAndAssert("floor(12.4343454)", "float1", 12, "failed on 12.4343454");
callAndAssert("floor(-12.4343454)", "float2", -13, "failed on -12.4343454");

/* ln */
callAndAssert("ln(1)", "positive", 0, "failed on positive number");
callAndAssert("ln(12.4343454)", "float1", 2.5204624341327104, "failed on 12.4343454");

/* natural logarithm on non-positive gives an error */
var resp = mldb.get("/v1/query", {q:"SELECT ln(value) from test where rowName()='zero'"});
assertContains(resp.json.error, "strictly positive");
assertEqual(resp.responseCode, 400, "expecting 400 on invalid input");

/* exp */
callAndAssert("exp(1)", "positive", 2.718281828459045, "failed on positive number");
callAndAssert("exp(0)", "zero", 1, "failed on zero");
callAndAssert("exp(12.4343454)", "float1", 251285.59500936428, "failed on 12.4343454");
callAndAssert("exp(-12.4343454)", "float2", 0.000003979535714980935, "failed on -12.4343454");

function bark(func)
{
    var resp = mldb.get("/v1/query", {q: "SELECT " + func + " FROM test"});
    assertEqual(resp.responseCode, 200, "GET failed");
    assertEqual(resp.json[0].columns[0][1], true, func + " failed");
}
/* quantize */
bark("quantize(2.17, 0.001) = 2.17");
bark("quantize(2.17, 0.01) = 2.17");
bark("quantize(2.17, 0.1) = 2.2");
bark("quantize(2.17, 1) = 2");
bark("quantize(2.17, 10) = 0");
bark("quantize(-0.1, 1) = 0");
bark("quantize(0, 10000) = 0");
bark("quantize(217, 0.1) = 217");
bark("quantize(217, 1) = 217");
bark("quantize(217, 10) = 220");
bark("quantize(217, 100) = 200");
bark("quantize(-217, 100) = -200");


"success"
