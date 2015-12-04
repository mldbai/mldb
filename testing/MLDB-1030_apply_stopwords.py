# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json, datetime

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : "toy"
}


dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()

dataset.record_row("elem1", [ ["title", "patate where when poire when", now]])
dataset.record_row("elem2", [ ["title", "allo where what he a allo", now]])

dataset.commit()


#add function
func_conf = {
    "type":"filter_stopwords",
    "params": {}
}
func_output = mldb.perform("PUT", "/v1/functions/stop", [], func_conf)
mldb.log(func_output)


# baggify our words
baggify_conf = {
    "type": "transform",
    "params": {
        "inputDataset": "toy",
        "outputDataset": {
            "id": "bag_of_words",
            "type": "sparse.mutable"
        },
        "select": """tokenize(title, {splitchars:' ', quotechar:'', min_token_length: 2}) as *""",
    }
}
baggify_output = mldb.perform("PUT", "/v1/procedures/baggify", [], baggify_conf)
mldb.log(baggify_output)

run_output = mldb.perform("POST", "/v1/procedures/baggify/runs")
mldb.log(run_output)
assert run_output["statusCode"] == 201

# query all
rez = mldb.perform("GET", "/v1/query", [["q", "select * from bag_of_words order by rowName() ASC"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)



def doCheck(myRez):
    words = [[x[0] for x in line["columns"]] for line in myRez]
    assert set(["patate", "poire"]) == set(words[0])
    assert ["allo"] == words[1]

# query while applying stopwords
rez = mldb.perform("GET", "/v1/query", [["q", "select stop({words: {*}})[words] as * from bag_of_words order by rowName() ASC"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)
doCheck(jsRez)


#####
# try both operations at once
rez = mldb.perform("GET", "/v1/query", [["q", """
    select stop({
                    words: tokenize(title, {min_token_length:2, splitchars: ' ', quotechar: ''})
                }
            )[words] as * 
    from toy 
    order by rowName() ASC"""]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)
doCheck(jsRez)


mldb.script.set_return("success")


