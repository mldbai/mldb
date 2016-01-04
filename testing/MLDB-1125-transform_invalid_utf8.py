# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json

conf = {
    "type":"text.csv.tabular",
    "params": {
        "dataFileUrl":"file:///home/mailletf/workspace/mldb/testing/dataset/invalid_utf8_tokenize.csv",
        "ignoreBadLines": False,
        "replaceInvalidCharactersWith": "_",
        "headers": ["action", "timestamp", "mod", "text"]
    }
}
res = mldb.perform("PUT", "/v1/datasets/utf8", [], conf)
mldb.log(res)


conf = {
    "type": "transform",
    "params": {
        "inputData": "select tokenize(text, {splitchars:' ', quotechar: '', ngram_range:[1,3]}) as * from utf8",
        "outputDataset": "bag_of_words",
        "runOnCreation": True
    }
}
res = mldb.perform("PUT", "/v1/procedures/token_it", [], conf)
mldb.log(res)

res = mldb.perform("GET", "/v1/query", [["q", "select * from bag_of_words limit 25"]])
jsRes = json.loads(res["response"])
mldb.log(jsRes)

mldb.log(" ======================== ")
mldb.log(" ======================== ")
mldb.log(" ======================== ")
mldb.log(" ======================== ")
mldb.log(" ======================== ")

conf = {
    "type": "transform",
    "params": {
        "inputData": "select text from utf8",
        "outputDataset": "bag_of_words",
        "runOnCreation": True
    }
}
res = mldb.perform("PUT", "/v1/procedures/token_it", [], conf)
mldb.log(res)





conf = {
    "type":"text.csv.tabular",
    "params": {
        "dataFileUrl":"file:///home/mailletf/workspace/mldb/mldb_data/community_sift_sample.csv",
        "ignoreBadLines": False,
        "headers": ["action", "timestamp", "moderator_name", "text"],
        "replaceInvalidCharactersWith": "?"
    }
}
mldb.perform("PUT", "/v1/datasets/sift", [], conf)

conf = {
    "type": "tokensplit",
    "params": {
        "dictionaryDataset": {
            "type": "text.csv.tabular",
            "params": {
                "dataFileUrl": "https://s3.amazonaws.com/public.mldb.ai/datasets/emoji.csv",
                "headers": ["emoji", "description"]
            }
        },
        "select": "emoji"
    }
}
mldb.perform("PUT", "/v1/functions/emojiSplit", [], conf)

res = mldb.perform("GET", "/v1/query", [["q", "select emojiSplit({text})[output] from sift where rowName() = 166198"]])
mldb.log(res)





mldb.script.set_return("success")
