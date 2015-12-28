# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json, random, datetime, os

conf = {
    "type": "sql.expression",
    "params": {
        "expression": """
            jseval(' 
                var result = {};
                result["msgLen"] = txt.length;
                return result;
            ', 'txt', CAST (text AS string)) as msgStats
        """
    }
}
rez = mldb.perform("PUT", "/v1/functions/getMsgStats", [], conf)


conf = {
    "type": "sql.expression",
    "params": {
        "expression": """
            getMsgStats({text: raw_text}) as *,
            tokenize(preProcessed, {splitchars: ' !'}) as words
        """
    }
}
rez = mldb.perform("PUT", "/v1/functions/getFeatVec", [], conf)

# This segfaults after a couple of calls
for i in range(25):
    rez = mldb.perform("GET", "/v1/query", 
                       [["q", """select getFeatVec({raw_text: 'I really loved this party!!!',
                       preProcessed: 'I really loved this party!!!'}) as *"""]])

jsRes = json.loads(rez["response"])
mldb.log(jsRes)

assert rez['statusCode'] == 200, 'expected query to return 200'
expected_response = [
   {
      "columns" : [
         [ "msgStats.msgLen", 28, "-Inf" ],
         [ "words.party", 1, "-Inf" ],
         [ "words.this", 1, "-Inf" ],
         [ "words.really", 1, "-Inf" ],
         [ "words.loved", 1, "-Inf" ],
         [ "words.I", 1, "-Inf" ]
      ]
   }
]
assert jsRes == expected_response, 'expected responses to match'

for i in range(25):
    rez = mldb.perform("GET", "/v1/query", 
                       [["q", """select getFeatVec({raw_text: 'I really loved this party!!!',
                       preProcessed: 'I really loved this party!!!'})"""]])

jsRes = json.loads(rez["response"])
assert rez['statusCode'] == 200, 'expected query to return 200'

mldb.log(jsRes)

mldb.script.set_return("success")
