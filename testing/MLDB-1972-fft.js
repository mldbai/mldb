// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var resp = mldb.query("select impulse(32) as i");

mldb.log(resp);

var resp = mldb.query("select fft(impulse(32), 'forward') as f");

mldb.log(resp);

var resp = mldb.query("select fft(shifted_impulse(32, 1), 'forward') as f");

mldb.log(resp);

mldb.log("shift", 0);

var resp = mldb.query("select amplitude(fft(shifted_impulse(32, 0), 'forward')) as f");

mldb.log(resp);

mldb.log("shift", 1);

var resp = mldb.query("select amplitude(fft(shifted_impulse(32, 1), 'forward')) as f");

mldb.log(resp);

mldb.log("shift", 2);

var resp = mldb.query("select amplitude(fft(shifted_impulse(32, 2), 'forward')) as f");

mldb.log(resp);

mldb.log("shift", 16);

var resp = mldb.query("select amplitude(fft(shifted_impulse(32, 16), 'forward')) as f");

mldb.log(resp);

var resp = mldb.query("select phase(fft(shifted_impulse(32, 2), 'forward')) / pi() * 180 as ph");

mldb.log(resp);

var resp = mldb.query("select fft(fft(impulse(32), 'forward'), 'backward') as f");

mldb.log(resp);

var resp = mldb.query("select fft(fft(shifted_impulse(32, 1), 'forward'), 'backward') as f");

mldb.log(resp);

var resp = mldb.query("select quantize(fft(fft(shifted_impulse(32, 31), 'forward'), 'backward'), 0.001) as f");

mldb.log(resp);

var resp = mldb.query("select quantize(fft(fft(shifted_impulse(32, 31), 'forward'), 'backward'), 0.001) = shifted_impulse(32, 31) as r");

mldb.log(resp);

// Make sure that the inverse of an FFT is equal to the original input
assertEqual(resp[0].columns[0][1], 1);

"success"
