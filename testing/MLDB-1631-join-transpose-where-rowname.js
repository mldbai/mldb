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

var resp = mldb.query('select * ' + 
                      'from (select \'this is toy story time\' as title) as y ' +
                      'join transpose((select {"toy story": 1, "terminator": 5} as * ' +
                      'named \'rating\')) as x ' +
                      'where regex_match(y.title, \'.*\'+x.rowName()+\'.*\')');

mldb.log(resp);

var expected = [
   {
      "columns" : [
         [ "x.rating", 1, "-Inf" ],
         [ "y.title", "this is toy story time", "-Inf" ]
      ],
      "rowName" : "[result]-[toy story]"
   }
];

assertEqual(resp, expected);

"success"

