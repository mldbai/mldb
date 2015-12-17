# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import datetime, json, random, math

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy'
}

dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()
dataset.record_row("br_1", [["host", "pataté.com", now], ["region", "qc", now], ["CLICK", "1", now]])
now += datetime.timedelta(seconds=1)
dataset.record_row("br_2", [["host", "poire.com", now], ["region", "on", now]])
now += datetime.timedelta(seconds=1)
dataset.record_row("br_3", [["host", "pataté.com", now], ["region", "on", now]])
dataset.commit()


def valForKey(lst, key):
    for row in lst:
        if row[0] == key:
            return row[1]
    raise Exception("Key not in list!")

for output_type, output_id in [("sparse.mutable", "out_beh"), ("sparse.mutable", "out_sparse")]:
    mldb.log("Running for id:%s type:%s" % (output_id, output_type))
    conf = {
        "type": "statsTable.train",
        "params": {
            "trainingDataset": "toy",
            "outputDataset": {"type": output_type, "id": output_id},
            "select": "* EXCLUDING(CLICK)",
            "outcomes": [["label", "CLICK IS NOT NULL"],
                       ["not_label", "CLICK IS NULL"]],
            "orderBy": "rowName() ASC",
            "statsTableFileUrl": "file://build/x86_64/tmp/mldb-873-stats_table.st",
            "functionName": "mySt"
        }
    }
    rez = mldb.perform("PUT", "/v1/procedures/myroll_%s" % output_id, [], conf)
    mldb.log(rez)

    rez = mldb.perform("POST", "/v1/procedures/myroll_%s/runs" % output_id)
    mldb.log(rez)

    rez = mldb.perform("GET", "/v1/query", [["q", "select * from %s order by rowName() ASC" % output_id]])
    jsResp = json.loads(rez["response"])
    mldb.log(jsResp)

    assert jsResp[0]["rowName"] == "br_1"
    assert valForKey(jsResp[2]["columns"], "label_region") == 0
    assert valForKey(jsResp[2]["columns"], "trial_region") == 1
    assert valForKey(jsResp[2]["columns"], "label_host") == 1
    
    assert valForKey(jsResp[2]["columns"], "not_label_region") == 1
    assert valForKey(jsResp[2]["columns"], "not_label_host") == 0


############
# Test the function
rez = mldb.perform("GET", "/v1/functions/mySt/application", [
    ["input", {
        "keys": {
            "host": "poire.com",
            "prout": "existe pas",
            "region": "verdun"
        }
    }]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

assert jsRez == {
        "output" : {
         "counts" : [
             ["trial_host", [ 1, "NaD" ]],
             ["label_host", [ 0, "NaD" ]],
             ["not_label_host", [ 1, "NaD" ]],
             ["trial_region", [ 0, "NaD" ]],
             ["label_region", [ 0, "NaD" ]],
             ["not_label_region", [ 0, "NaD" ]]
          ]}}



#########
# Test the function within a select statement
rez = mldb.perform("GET", "/v1/query", [["q", "select mySt({{*} as keys}) AS * from toy order by rowName() ASC"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

assert valForKey(jsRez[0]["columns"], "counts.label_region") == 1
assert valForKey(jsRez[1]["columns"], "counts.label_region") == 0

assert valForKey(jsRez[1]["columns"], "counts.trial_host") == 1
assert valForKey(jsRez[2]["columns"], "counts.trial_host") == 2




#######
# Test the derived columns procedure
conf = {
    "type": "experimental.statsTable.derivedColumnsGenerator",
    "params": {
        "expression": """
                        counts.label as lbl_hoho_$tbl,
                        counts.label as lbl_$tbl,
                        counts.label/counts.trial as ctr_$tbl,
                        1 as pwet_$tbl,
                        ln(counts.trial+1) as hoho_$tbl""",
        "statsTableFileUrl": "file://build/x86_64/tmp/mldb-873-stats_table.st",
        "functionId": "getDerived"
    }
}
rez = mldb.perform("PUT", "/v1/procedures/getDerivedGen", [], conf)
mldb.log(rez)
rez = mldb.perform("POST", "/v1/procedures/getDerivedGen/runs")
mldb.log(rez)

rez = mldb.perform("GET", "/v1/functions/getDerived")
jsRez = json.loads(rez["response"])
mldb.log(jsRez)



def assertValForCol(rows, key, goodVal):
    for rowName, rowVal, rowTs in rows:
        if rowName == key:
            assert abs(rowVal - goodVal) < 0.001
            return True
    mldb.log(str(rows))
    raise Exception("Could not find key: " + key)

def assertForRows(rows, name, col, goodVal):
    for row in rows:
        if row["rowName"] == name:
            return assertValForCol(row["columns"], col, goodVal)
    
    raise Exception("Could not find row: " + name)



#########
# Test the function within a select statement
rez = mldb.perform("GET", "/v1/query", [["q", "select getDerived({counts: {label_host:5, trial_host: 500, label_region:0, trial_region:250}}) as *"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

assertValForCol(jsRez[0]["columns"], "ctr_host", 5/500.)
assertValForCol(jsRez[0]["columns"], "ctr_region", 0)
assertValForCol(jsRez[0]["columns"], "pwet_host", 1)



rez = mldb.perform("GET", "/v1/query", [["q", "select mySt({keys: {*}}) as * from toy order by rowName() ASC limit 1"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

rez = mldb.perform("GET", "/v1/query", [["q", "select getDerived({mySt({keys: {*}}) as *}) as * from toy order by rowName() ASC limit 1"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

assertValForCol(jsRez[0]["columns"], "ctr_host", 1/2.)
assertValForCol(jsRez[0]["columns"], "ctr_region", 1)
assertValForCol(jsRez[0]["columns"], "hoho_host", math.log(3))



######
# BagOfWordsStatsTable Test

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'posneg'
}

dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()
dataset.record_row("a", [["text", "I like apples", now], ["CLICK", "1", now]])
dataset.record_row("b", [["text", "I like Macs", now]])
dataset.record_row("c", [["text", "What about bananas?", now]])
dataset.record_row("d", [["text", "Apples are red", now], ["CLICK", "1", now]])
dataset.record_row("e", [["text", "Bananas are yellow", now]])
dataset.record_row("f", [["text", "Oranges are ... orange", now]])
dataset.commit()


conf = {
    "type": "bagOfWordStatsTable.train",
    "params": {
        "trainingDataset": "posneg",
        "select": "tokenize(text, {splitchars: ' '}) as *",
        "outcomes": [["label", "CLICK IS NOT NULL"]],
        "statsTableFileUrl": "file://build/x86_64/tmp/mldb-873-stats_table_posneg.st",
    }
}
rez = mldb.perform("PUT", "/v1/procedures/myroll_posneg_%s" % output_id, [], conf)
mldb.log(rez)
rez = mldb.perform("POST", "/v1/procedures/myroll_posneg_%s/runs" % output_id)
mldb.log(rez)


conf = {
    "type": "bagOfWordStatsTable.posneg",
    "params": {
        "numPos": 4,
        "numNeg": 4,
        "minTrials": 1,
        "outcomeToUse": "label",
        "statsTableFileUrl": "file://build/x86_64/tmp/mldb-873-stats_table_posneg.st",
    }
}
rez = mldb.perform("PUT", "/v1/functions/posnegz", [], conf)
mldb.log(rez)

rez = mldb.perform("GET", "/v1/query", [["q", "select posnegz({words: tokenize(text, {splitchars: ' .'})}) as * from posneg"]])
jsRez = json.loads(rez["response"])
mldb.log(jsRez)

assertForRows(jsRez, "d", "probs.red_label", 1)
assertForRows(jsRez, "a", "probs.I_label", 0.5)
assertForRows(jsRez, "b", "probs.I_label", 0.5)



mldb.script.set_return("success")

