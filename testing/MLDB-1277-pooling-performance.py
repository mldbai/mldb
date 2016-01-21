# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json


dataset = mldb.create_dataset({ 
    "id": "reddit_raw", "type": "text.line",  
    "params": { 
        "dataFileUrl": "http://files.figshare.com/1310438/reddit_user_posting_behavior.csv.gz" 
    } 
})


result = mldb.perform("PUT", "/v1/procedures/import", [], {
    "type": "transform",
    "params": {
        "inputData": "select tokenize(lineText, {offset: 1, value: 1}) as * from reddit_raw",
        "outputDataset": "reddit_dataset",
        "runOnCreation": True
    }
})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("PUT", "/v1/procedures/svd", [], {
    "type" : "svd.train",
    "params" : {
        "trainingData" : """
            SELECT 
                COLUMN EXPR (AS columnName() ORDER BY rowCount() DESC, columnName() LIMIT 4000) 
            FROM reddit_dataset
        """,
        "columnOutputDataset" : "reddit_svd_embedding",
        "runOnCreation": True
    }
})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("PUT", "/v1/procedures/rename", [], {
    "type" : "transform",
    "params" : {
        "inputData" : "select * named regex_replace(rowName(), '\|1', '') from reddit_svd_embedding",
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

def query(sql):
    result = mldb.perform("GET", "/v1/query", [["q", sql], ["format", "table"]], {})
    assert result["statusCode"] < 400, result["response"]
    return result["response"]

mldb.log('start')
query("""
select count(*) from (
    select 
    pooler({words: tokenize(lineText)})[embedding] as x
    from reddit_raw limit 1000
)
""")
mldb.log('stop')

mldb.script.set_return("success")

