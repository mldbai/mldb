// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

function expectEmpty(query)
{
    var expected = [];
    var resp = mldb.query(query);

    mldb.log(resp);

    unittest.assertEqual(resp, expected);
}

function expectFound(query)
{
    var expected = [
        {
            "columns" : [
                [ "x", 1, "-Inf" ]
            ],
            "rowName" : "\"msnbc.com\""
        }
    ];

    var resp = mldb.query(query);

    mldb.log(resp);

    unittest.assertEqual(resp, expected);
}

// Defeat optimization to use slow path
expectEmpty("select * from (select 1 as x named 'msnbc.com') where rowName() + '' = 'msnbc.com'");
// Fast path for rowName = ...
expectEmpty("select * from (select 1 as x named 'msnbc.com') where rowName() = 'msnbc.com'");
// Fast path for rowName = ...
expectFound("select * from (select 1 as x named 'msnbc.com') where rowName() = '\"msnbc.com\"'");
// Check no exception when invalid rowName (unbalanced quotes)
expectEmpty("select * from (select 1 as x named 'msnbc.com') where rowName() = '\"msnbc.com'");
// Check no exception when invalid rowName (empty)
expectEmpty("select * from (select 1 as x named 'msnbc.com') where rowName() = ''");
// Check in (...)
expectFound("select * from (select 1 as x named 'msnbc.com') where rowName() in ('\"msnbc.com\"')");
expectFound("select * from (select 1 as x named 'msnbc.com') where rowName() in ('\"msnbc.com\"', 'msnbc.com', '\"')");
expectEmpty("select * from (select 1 as x named 'msnbc.com') where rowName() in ('msnbc.com', '\"')");
expectFound("select * from (select 1 as x named 'msnbc.com') where true and rowName() != 'msnbc.com'");
expectEmpty("select * from (select 1 as x named 'msnbc.com') where rowName() != '\"msnbc.com\"' + ''");
expectEmpty("select * from (select 1 as x named 'msnbc.com') where true and rowName() != '\"msnbc.com\"'");


// rowPath()
expectFound("select * from (select 1 as x named 'msnbc.com') where rowPath() = 'msnbc.com'");
expectEmpty("select * from (select 1 as x named 'msnbc.com') where rowPath() = '\"msnbc.com\"'");
expectFound("select * from (select 1 as x named 'msnbc.com') where rowPath() = 'msnbc.com' + ''");
// Check no exception when invalid rowPath (empty)
expectEmpty("select * from (select 1 as x named 'msnbc.com') where rowPath() = ''");
// Check in (...)
expectFound("select * from (select 1 as x named 'msnbc.com') where rowPath() in ('msnbc.com')");
expectFound("select * from (select 1 as x named 'msnbc.com') where rowPath() in ('msnbc.com', null, [ 1.2, 3.4, 5.6])");
expectEmpty("select * from (select 1 as x named 'msnbc.com') where true and rowPath() != 'msnbc.com'");
expectFound("select * from (select 1 as x named 'msnbc.com') where rowPath() != '\"msnbc.com\"'");


// Tests to add
// rowName() in (keys of xxx)
// rowName() in (values of xxx)
// rowPath() in (keys of xxx)
// rowPath() in (values of xxx)
// Non-canonical rowName() should return no rows, eg '...' should not match "".""."" since we match as strings on rowName, not structured



"success"
