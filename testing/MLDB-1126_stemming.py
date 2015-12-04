# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#
# MLDB-1126_stemming.py
# Francois Maillet, 2015-11-19
# Copyright (c) 2015 Datacratic Inc. All rights reserved.
#

import json

def find_column(response, column, value):
    for col in response[0]["columns"]:
        if col[0] == column:
            assert col[1] == value
            return

    assert False



conf = {
    "type": "stemmer",
    "params": {
        "language": "english"
    }
}
res = mldb.perform("PUT", "/v1/functions/stemmer", [], conf)
mldb.log(res)


result = mldb.perform('GET', '/v1/query', [['q', "SELECT stemmer({words: {tokenize('I like having lots', {splitchars:' '}) as *}}) as *"]])
jsRes = json.loads(result["response"])
mldb.log(jsRes)

find_column(jsRes, "words.lot", 1)
find_column(jsRes, "words.have", 1)
find_column(jsRes, "words.I", 1)



conf = {
    "type": "stemmer",
    "params": {
        "language": "french"
    }
}
res = mldb.perform("PUT", "/v1/functions/stemmer_fr", [], conf)
mldb.log(res)


result = mldb.perform('GET', '/v1/query', [['q', "SELECT stemmer_fr({words: {tokenize('Je aimé aimer aimerais les chiens', {splitchars:' '}) as *}}) as *"]])
jsRes = json.loads(result["response"])
mldb.log(jsRes)

find_column(jsRes, "words.aim", 3)
find_column(jsRes, "words.le", 1)

#MLDB-1147 stemmer on document

conf = {
    "type": "stemmerdoc",
    "params": {
        "language": "english"
    }
}
res = mldb.perform("PUT", "/v1/functions/stemmerdoc", [], conf)
mldb.log(res)

result = mldb.perform('GET', '/v1/query', [['q', "SELECT stemmerdoc({document: 'I like having lots'}) as output"]])
jsRes = json.loads(result["response"])
mldb.log(jsRes)

find_column(jsRes, "output.document", "I like have lot")

mldb.script.set_return("success")

