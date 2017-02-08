// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

function callAndAssert(func, expected, message)
{
    var resp = mldb.get('/v1/query', {q:"SELECT " + func});
    assertEqual(resp.responseCode, 200, "GET /v1/query failed");
    assertEqual(resp.json[0].columns[0][1], expected, func + "   " +  message);
}

function assertError(select, code, error) {
    var resp = mldb.get('/v1/query', {q:"SELECT " + select});
    assertEqual(resp.responseCode, code, "wrong code");
    assertEqual(resp.json.error, error, "wrong error");
}

/* abs */
callAndAssert("abs(1)", 1, "failed on positive number");
callAndAssert("abs(0)", 0, "failed on zero");
callAndAssert("abs(-1)", 1, "failed on negative number");

/* power */
callAndAssert("power(1,2)", 1, "failed on positive number");
callAndAssert("power(0,2)", 0, "failed on zero");
callAndAssert("power(4,2)", 16, "failed on four");
callAndAssert("power(-1,2)", 1, "failed on negative number");

/* sqrt */
callAndAssert("sqrt(-1)", {"num": "-NaN"}, "failed on negative number");
callAndAssert("sqrt(1)", 1, "failed on positive number");
callAndAssert("sqrt(0)", 0, "failed on zero");
callAndAssert("sqrt(4)", 2, "failed on four");

/* sqrt undo power and vice versa */
callAndAssert("sqrt(power(1, 2))", 1, "failed on positive number");
callAndAssert("sqrt(power(0, 2))", 0, "failed on zero");
callAndAssert("sqrt(power(4,2))", 4, "failed on four");

/* modulus */
callAndAssert("mod(1, 2)", 1, "failed on positive number");
callAndAssert("mod(0, 2)", 0, "failed on zero");
callAndAssert("mod(4,2)", 0, "failed on four");
callAndAssert("mod(-1,2)", -1, "mod failed on -1");

/* ceiling */
callAndAssert("ceil(1)", 1, "failed on positive number");
callAndAssert("ceil(0)", 0, "failed on zero");
callAndAssert("ceil(12.4343454)", 13, "failed on 12.4343454");
callAndAssert("ceil(-12.4343454)", -12, "failed on -12.4343454");

/* floor */
callAndAssert("floor(1)", 1, "failed on positive number");
callAndAssert("floor(0)", 0, "failed on zero");
callAndAssert("floor(12.4343454)", 12, "failed on 12.4343454");
callAndAssert("floor(-12.4343454)", -13, "failed on -12.4343454");

/* logarithms */
callAndAssert("ln(-1)", {"num":"NaN"}, "failed on positive number");
callAndAssert("ln(0)", {"num":"-Inf"}, "failed on positive number");
callAndAssert("ln(1)", 0, "failed on positive number");
callAndAssert("ln(2)", 0.6931471805599453094172 , "failed on 12.4343454"); 
callAndAssert("ln(NULL)", null, "failed on null"); 

callAndAssert("log(-1)", {"num":"NaN"}, "log(b,x) failed");
callAndAssert("log(0)", {"num":"-Inf"}, "log(b,0) failed");
callAndAssert("log(1)", 0, "log(1) failed");
callAndAssert("log(1000)", 3, "log(1000) failed");
callAndAssert("log(NULL)", null, "failed on null"); 

callAndAssert("log(2, -1)", {"num":"NaN"}, "log(b,x) failed");
callAndAssert("log(2, 0)", {"num":"-Inf"}, "log(b,0) failed");
callAndAssert("log(2, 1)", 0, "log(b,1) failed");
callAndAssert("log(2, 16)", 4, "log(b,16) failed");
callAndAssert("log(2, sqrt(2))", 0.5000000000000001, "log(b,x) failed");
callAndAssert("log(2, NULL)", null, "failed on null"); 
callAndAssert("log(NULL, 2)", null, "failed on null"); 

assertError("log(1,2,3)", 400, "Binding builtin function log: the log"
                               + " function expected 1 or 2 arguments, got 3");

/* exp */
callAndAssert("exp(1)", 2.718281828459045, "failed on positive number");
callAndAssert("exp(0)", 1, "failed on zero");
callAndAssert("exp(12.4343454)", 251285.59500936428, "failed on 12.4343454");
callAndAssert("exp(-12.4343454)", 0.000003979535714980935, "failed on -12.4343454");

function bark(func)
{
    callAndAssert(func, true, "quantize failed");
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
