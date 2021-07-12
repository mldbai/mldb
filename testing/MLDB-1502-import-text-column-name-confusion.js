// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var importConfig = {
    type: 'import.text',
    params: {
        dataFileUrl: 'file://mldb/mldb_test_data/reddit.csv.zst',
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

unittest.assertEqual(resp.responseCode, 201);

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

unittest.assertEqual(resp.json, expected);

"success"
