# -*- coding: utf-8 -*-
#
# MLDB-1126_stemming.py
# Francois Maillet, 2015-11-19
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

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
res = mldb.put("/v1/functions/stemmer", conf)
mldb.log(res)


result = mldb.get(
    '/v1/query',
    q="SELECT stemmer("
      "{words: {tokenize('I like having lots', {splitChars:' '}) as *}}) as *")
js_res = result.json()
mldb.log(js_res)

find_column(js_res, "words.lot", 1)
find_column(js_res, "words.have", 1)
find_column(js_res, "words.I", 1)



conf = {
    "type": "stemmer",
    "params": {
        "language": "french"
    }
}
res = mldb.put("/v1/functions/stemmer_fr", conf)
mldb.log(res)


result = mldb.get(
    '/v1/query',
    q=unicode("SELECT stemmer_fr({words: {tokenize("
              "'Je aimé aimer aimerais les chiens', {splitChars:' '}) as *}}) "
              "as *", encoding='utf-8'))
js_res = result.json()
mldb.log(js_res)

find_column(js_res, "words.aim", 3)
find_column(js_res, "words.le", 1)

#MLDB-1147 stemmer on document

conf = {
    "type": "stemmerdoc",
    "params": {
        "language": "english"
    }
}
res = mldb.put("/v1/functions/stemmerdoc", conf)
mldb.log(res)

result = mldb.get(
    '/v1/query',
    q="SELECT stemmerdoc("
      "{document: 'I like having lots'}) as output")
js_res = result.json()
mldb.log(js_res)

find_column(js_res, "output.document", "I like have lot")


conf = {
    "type": "stemmerdoc",
    "params": {
        "language": "french"
    }
}
res = mldb.put("/v1/functions/stemmerdoc_fr", conf)
mldb.log(res)

result = mldb.get(
    '/v1/query',
    q=unicode("SELECT stemmerdoc_fr("
              "{document: 'Je aimé aimer François'}) as output",
              encoding='utf-8'))
js_res = result.json()
mldb.log(js_res)

find_column(js_res, "output.document", unicode("Je aim aim François", "utf-8"))

# Now an example where the column values are counts
#
# potato | potatoes       -- stemming -->      potato
#   2    |    3                                  5
#
res = mldb.get(
    '/v1/query',
    q='SELECT stemmer({words:{*}})[words] as * FROM'
      ' (SELECT 2 as potato, 3 as potatoes)')
js_res = res.json()
find_column(js_res, 'potato', 5)
mldb.log(js_res)

# string value should be treated as a "1", true=1 and false=0
res = mldb.get(
    '/v1/query',
    q='SELECT stemmer({words:{*}})[words] as * FROM'
      " (SELECT false as potato, 3 as potatoes, true as carrot, 'oui' as carrots)")
js_res = res.json()
find_column(js_res, 'potato', 3)
find_column(js_res, 'carrot', 2)

mldb.script.set_return("success")
