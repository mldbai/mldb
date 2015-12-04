# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json

conf = {
    "type":"import.sentiwordnet",
    "params": {
        "dataFileUrl": 'file:///home/mailletf/workspace/mldb/mldb_data/SentiWordNet_3.0.0_20130122.txt.gz',
        "outputDataset": {
            "type": 'embedding',
            "id": 'sentiWordNet'
        },
    }
}
rez = mldb.perform("PUT", "/v1/procedures/lets_get_sentimental", [], conf)
mldb.log(rez)
assert rez["statusCode"] == 201

rez = mldb.perform("POST", "/v1/procedures/lets_get_sentimental/runs")
mldb.log(rez)
assert rez["statusCode"] == 201

rez = mldb.perform("GET", "/v1/query", [["q", "select * from sentiWordNet where rowName() IN ('love#v', 'dog#n')"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

# print the 5 most positive words
rez = mldb.perform("GET", "/v1/query", [["q", "select * from sentiWordNet order by PosSenti DESC limit 5"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

# print the 5 most negative words
rez = mldb.perform("GET", "/v1/query", [["q", "select * from sentiWordNet order by NegSenti DESC limit 5"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)


rez = mldb.perform("GET", "/v1/datasets/sentiWordNet/routes/rowNeighbours", [["row", "blue#a"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)


def checkWord(word, good):
    rez = mldb.perform("GET", "/v1/query", [["q", "select * from sentiWordNet where rowName() = '%s'" % word]])
    jsRez = json.loads(rez["response"])
    mldb.log(jsRez)

    cols = jsRez[0]["columns"]
    pos = cols[0][1]
    neg = cols[1][1]
    mldb.log("Word: %s    Good:%0.8f     Us: %0.8f - %0.8f = %0.8f" % (word, good, pos, neg, pos-neg))

    # check that the difference is small
    assert abs(good - (pos - neg)) < 0.001


checkWord("good#a", 0.6337632198238539)
checkWord("bad#a", -0.5706406664316871)
checkWord("blue#a", -0.21950284713096807)
checkWord("blue#n", 0)

mldb.script.set_return("success")

