// This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

var mldb = require('mldb')
var unittest = require('mldb/unittest')

var importConfig = {
    type: 'import.text',
    params: {
        dataFileUrl: 'https://public.mldb.ai/reddit.csv.gz',
        named: "jseval('return x.substr(0, x.indexOf('',''));', 'x', lineText)",
        outputDataset: { id: 'reddit_text_file' },
        limit: 1000,
        delimiter: "",
        quoteChar: "",
        runOnCreation: true
    },
};

var resp = mldb.put('/v1/procedures/import', importConfig);

mldb.log(resp);

unittest.assertEqual(resp.responseCode, 201);

var resp = mldb.get('/v1/query', { q: 'select * from reddit_text_file order by rowName() asc limit 2', format: 'table' });

mldb.log(resp.json);

var expected = [
   [ "_rowName", "lineText" ],
   [
      "1000",
      "1000,television,politics,askscience,movies,TheContinuum,asoiaf,AskReddit,WTF,pics,TrueReddit,funny,offmychest,gaming,atheism,TrueAtheism,Minecraft360,Music,breakingbad,Hungergames,videos,scifi,xbox360,renfaire,Minecraft,DebateReligion,startrek"
   ],
   [
      "1002",
      "1002,politics,space,news,todayilearned,Drugs,WikiLeaks,environment,progressive,RedditDayOf,economy,business,hacking,science,worldnews,technology,obama,spaceporn"
   ]
];

unittest.assertEqual(resp.json, expected);

"success"
