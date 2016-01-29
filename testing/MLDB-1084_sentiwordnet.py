#
# python_mldb_interface_test.py
# Datacratic, 2015
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

conf = {
    "type": "import.sentiwordnet",
    "params": {
        "dataFileUrl": 's3://dev.mldb.datacratic.com/tests/MLDB-1084/SentiWordNet_3.0.0_20130122.txt.gz',
        "outputDataset": {
            "type": 'embedding',
            "id": 'sentiWordNet'
        },
    }
}
rez = mldb.put("/v1/procedures/lets_get_sentimental", conf)
mldb.log(rez)

rez = mldb.post("/v1/procedures/lets_get_sentimental/runs")
mldb.log(rez)

rez = mldb.get(
    "/v1/query",
    q="select * from sentiWordNet where rowName() IN ('love#v', 'dog#n')")
mldb.log(rez.json())

# print the 5 most positive words
rez = mldb.get(
    "/v1/query",
    q="select * from sentiWordNet order by PosSenti DESC limit 5")
mldb.log(rez.json())

# print the 5 most negative words
rez = mldb.get("/v1/query",
               q="select * from sentiWordNet order by NegSenti DESC limit 5")
mldb.log(rez.json())


rez = mldb.get("/v1/datasets/sentiWordNet/routes/rowNeighbours", row='blue#a')
mldb.log(rez.json())


def check_word(word, good):
    rez = mldb.get(
        "/v1/query",
        q="select * from sentiWordNet where rowName() = '%s'" % word)
    js_rez = rez.json()
    mldb.log(js_rez)

    cols = js_rez[0]["columns"]
    pos = cols[0][1]
    neg = cols[1][1]
    mldb.log("Word: %s    Good:%0.8f     Us: %0.8f - %0.8f = %0.8f"
             % (word, good, pos, neg, pos-neg))

    # check that the difference is small
    assert abs(good - (pos - neg)) < 0.001


check_word("good#a", 0.6337632198238539)
check_word("bad#a", -0.5706406664316871)
check_word("blue#a", -0.21950284713096807)
check_word("blue#n", 0)

mldb.script.set_return("success")
