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

var importConfig = {
    type: 'import.text',
    params: {
        dataFileUrl: 'https://public.mldb.ai/reddit.csv.gz',
        outputDataset: { id: 'reddit_text_file' },
        limit: 1000,
        delimiter: "",
        quoteChar: "",
        headers: ['customLine'],
        runOnCreation: true
    }
};

var resp = mldb.put('/v1/procedures/import', importConfig);

mldb.log(resp);

assertEqual(resp.responseCode, 201);

var resp = mldb.get('/v1/query', { q: 'select * from reddit_text_file order by rowName() asc limit 2', format: 'table' });

mldb.log(resp.json);

var expected = [
   [ "_rowName", "customLine" ],
   [ "1", "603,politics,trees,pics" ],
   [
      "10",
      "612,politics,2012Elections,Parenting,IAmA,fresno,picrequests,AskReddit,loseit,WTF,Marriage,Mommit,pics,funny,VirginiaTech,loseit_classic,RedditLaqueristas,atheism,LadyBoners,GradSchool"
   ]
];

assertEqual(resp.json, expected);

"success"
