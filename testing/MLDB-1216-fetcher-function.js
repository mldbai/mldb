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

var res = mldb.put('/v1/functions/fetch', { type: 'fetcher' });

mldb.log(res);

assertEqual(res.responseCode, 201);

var getCountryConfig = {
    type: 'sql.expression',
    params: {
        expression: "extract_column('geoplugin_countryCode', parse_json(CAST (fetch({url: 'http://www.geoplugin.net/json.gp?ip=' + ip})[content] AS STRING))) as country"
    }
};

var res = mldb.put('/v1/functions/getCountry', getCountryConfig);

mldb.log(res);

assertEqual(res.responseCode, 201);

var res = mldb.get('/v1/query', { q: "SELECT getCountry({ip: '158.245.13.123'}) AS *", format: 'table'});

mldb.log(res.json);

assertEqual(res.json[1][1], "US");

"success"
