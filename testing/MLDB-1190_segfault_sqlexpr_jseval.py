#
# MLDB-1190_segfault_sqlexpr_jseval.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

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
mldb.put("/v1/functions/getMsgStats", conf)


conf = {
    "type": "sql.expression",
    "params": {
        "expression": """
            getMsgStats({text: raw_text}) as *,
            tokenize(preProcessed, {splitChars: ' !'}) as words
        """
    }
}
mldb.put("/v1/functions/getFeatVec", conf)

# This segfaults after a couple of calls
for i in range(25):
    res = mldb.get(
        "/v1/query",
        q="""select getFeatVec({raw_text: 'I really loved this party!!!',
             preProcessed: 'I really loved this party!!!'}) as *""")

expected_response = [
    {
        "rowName": "result", 
        "columns": [
            [
                "msgStats.msgLen", 
                28, 
                "-Inf"
            ], 
            [
                "words.I", 
                1, 
                "-Inf"
            ], 
            [
                "words.loved", 
                1, 
                "-Inf"
            ], 
            [
                "words.party", 
                1, 
                "-Inf"
            ], 
            [
                "words.really", 
                1, 
                "-Inf"
            ], 
            [
                "words.this", 
                1, 
                "-Inf"
            ]
        ]
    }
]

mldb.log(res.json())

assert res.json() == expected_response, 'expected responses to match'

for i in range(25):
    rez = mldb.get(
        "/v1/query",
        q="""select getFeatVec({raw_text: 'I really loved this party!!!',
             preProcessed: 'I really loved this party!!!'})""")

mldb.script.set_return("success")
