# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json


dataset = mldb.create_dataset({ 
    "id": "reddit_raw", "type": "text.line",  
    "params": { 
        "dataFileUrl": "http://files.figshare.com/1310438/reddit_user_posting_behavior.csv.gz" 
    } 
})

dataset = mldb.create_dataset({ 
    "id": "reddit_svd_embedding", 
    "type": "text.csv.tabular",  
    "params": { 
        "dataFileUrl": "https://s3.amazonaws.com/public.mldb.ai/reddit_embedding.csv.gz",
    } 
})


result = mldb.perform("PUT", "/v1/procedures/rename", [], {
    "type" : "transform",
    "params" : {
        "inputData" : "select * excluding(name) named name from reddit_svd_embedding",
        "outputDataset" : {"id":"reddit_svd_embedding2", "type":"embedding"},
        "runOnCreation": True
    }
})
assert result["statusCode"] < 400, result["response"]


result = mldb.perform("PUT", "/v1/functions/pooler", [], {
    "type": "pooling",
    "params": {
        "embeddingDataset": "reddit_svd_embedding2"
    }
})
assert result["statusCode"] < 400, result["response"]


result = mldb.perform("PUT", "/v1/functions/wrapper", [], {
    "type": "sql.expression",
    "params": {
        "expression": "pooler({words: tokenize(lineText)})[embedding] as x"
    }
})
assert result["statusCode"] < 400, result["response"]


def query(sql):
    result = mldb.perform("GET", "/v1/query", [["q", sql], ["format", "table"]], {})
    assert result["statusCode"] < 400, result["response"]
    return result["response"]

mldb.log('start')
query("""

    select wrapper({lineText}) from reddit_raw limit 100000

""")
mldb.log('stop')

mldb.script.set_return("success")

