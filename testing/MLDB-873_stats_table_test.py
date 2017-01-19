# -*- coding: utf-8 -*-
#
# MLDB-873_stats_table_test.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
import datetime, math

mldb = mldb_wrapper.wrap(mldb) # noqa

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy'
}

dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()
dataset.record_row("br_1", [["host", "pataté.com", now], ["region", "qc", now],
                            ["CLICK", "1", now]])
now += datetime.timedelta(seconds=1)
dataset.record_row("br_2", [["host", "poire.com", now], ["region", "on", now]])
now += datetime.timedelta(seconds=1)
dataset.record_row("br_3", [["host", "pataté.com", now],
                            ["region", "on", now]])
dataset.commit()


def val_for_key(lst, key):
    for row in lst:
        if row[0] == key:
            return row[1]
    raise Exception("Key not in list!")

for output_type, output_id in [("sparse.mutable", "out_beh"),
                               ("sparse.mutable", "out_sparse")]:
    mldb.log("Running for id:%s type:%s" % (output_id, output_type))
    conf = {
        "type": "statsTable.train",
        "params": {
            "trainingData": "select * EXCLUDING(CLICK) from toy order by rowName() ASC",
            "outputDataset": {"type": output_type, "id": output_id},
            "outcomes": [["label", "CLICK IS NOT NULL"],
                       ["not_label", "CLICK IS NULL"]],
            "statsTableFileUrl": "file://build/x86_64/tmp/mldb-873-stats_table.st",
            "functionName": "mySt",
            "runOnCreation": True
        }
    }
    rez = mldb.put("/v1/procedures/myroll_%s" % output_id, conf)
    mldb.log(rez.json())

   # rez = mldb.post("/v1/procedures/myroll_%s/runs" % output_id)
   # mldb.log(rez)

    rez = mldb.get("/v1/query",
                   q="select * from %s order by rowName() ASC" % output_id)
    js_resp = rez.json()
    mldb.log(js_resp)

    assert js_resp[0]["rowName"] == "br_1"
    assert val_for_key(js_resp[2]["columns"], "label.region") == 0
    assert val_for_key(js_resp[2]["columns"], "trial.region") == 1
    assert val_for_key(js_resp[2]["columns"], "label.host") == 1

    assert val_for_key(js_resp[2]["columns"], "not_label.region") == 1
    assert val_for_key(js_resp[2]["columns"], "not_label.host") == 0


############
# Test the function
rez = mldb.get("/v1/functions/mySt/application",
    input={
        "keys": {
            "host": "poire.com",
            "prout": "existe pas",
            "region": "verdun"
        }
    })
js_rez = rez.json()
mldb.log(js_rez)

assert js_rez == {
    "output": {
        "counts": [
            [
                "label", 
                [
                    [
                        [
                            "host", 
                            [
                                0, 
                                "NaD"
                            ]
                        ], 
                        [
                            "region", 
                            [
                                0, 
                                "NaD"
                            ]
                        ]
                    ], 
                    "NaD"
                ]
            ], 
            [
                "not_label", 
                [
                    [
                        [
                            "host", 
                            [
                                1, 
                                "NaD"
                            ]
                        ], 
                        [
                            "region", 
                            [
                                0, 
                                "NaD"
                            ]
                        ]
                    ], 
                    "NaD"
                ]
            ], 
            [
                "trial", 
                [
                    [
                        [
                            "host", 
                            [
                                1, 
                                "NaD"
                            ]
                        ], 
                        [
                            "region", 
                            [
                                0, 
                                "NaD"
                            ]
                        ]
                    ], 
                    "NaD"
                ]
            ]
        ]
    }
}


#########
# Test the function within a select statement
rez = mldb.get(
    "/v1/query",
    q="select mySt({{*} as keys}) AS * from toy order by rowName() ASC")
js_rez = rez.json()
mldb.log(js_rez)

assert val_for_key(js_rez[0]["columns"], "counts.label.region") == 1
assert val_for_key(js_rez[1]["columns"], "counts.label.region") == 0

assert val_for_key(js_rez[1]["columns"], "counts.trial.host") == 1
assert val_for_key(js_rez[2]["columns"], "counts.trial.host") == 2


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
rez = mldb.put("/v1/procedures/getDerivedGen", conf)
mldb.log(rez.json())
rez = mldb.post("/v1/procedures/getDerivedGen/runs")
mldb.log(rez.json())

rez = mldb.get("/v1/functions/getDerived")
js_rez = rez.json()
mldb.log(js_rez)

def assert_val_for_col(cols, key, goodVal):
    for colName, colVal, colTs in cols:
        if colName == key:
            assert abs(colVal - goodVal) < 0.001
            return True
    mldb.log(str(cols))
    raise Exception("Could not find key: " + key)

def assert_for_rows(rows, rowName, col, goodVal):
    for row in rows:
        if row["rowName"] == rowName:
            return assert_val_for_col(row["columns"], col, goodVal)

    raise Exception("Could not find row: " + rowName)

#########
# Test the function within a select statement
rez = mldb.get(
    "/v1/query",
    q="select getDerived({counts: {label: {host:5, region: 0}, trial: {host: 500, region: 250 } }}) as *")
js_rez = rez.json()
mldb.log(js_rez)

assert_val_for_col(js_rez[0]["columns"], "ctr_host", 5/500.)
assert_val_for_col(js_rez[0]["columns"], "ctr_region", 0)
assert_val_for_col(js_rez[0]["columns"], "pwet_host", 1)

rez = mldb.get(
    "/v1/query",
    q="select getDerived({counts: {label.host:5, trial.host: 500, label.region: 0, trial.region: 250}}) as *")
js_rez = rez.json()
mldb.log(js_rez)

assert_val_for_col(js_rez[0]["columns"], "ctr_host", 5/500.)
assert_val_for_col(js_rez[0]["columns"], "ctr_region", 0)
assert_val_for_col(js_rez[0]["columns"], "pwet_host", 1)

rez = mldb.get(
    "/v1/query",
    q="select mySt({keys: {*}}) as * from toy order by rowName() ASC limit 1")
js_rez = rez.json()
mldb.log(js_rez)

rez = mldb.get(
    "/v1/query",
    q="select getDerived({mySt({keys: {*}}) as *}) as * from toy order by rowName() ASC limit 1")
js_rez = rez.json()
mldb.log(js_rez)

assert_val_for_col(js_rez[0]["columns"], "ctr_host", 1/2.)
assert_val_for_col(js_rez[0]["columns"], "ctr_region", 1)
assert_val_for_col(js_rez[0]["columns"], "hoho_host", math.log(3))

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
    "type": "statsTable.bagOfWords.train",
    "params": {
        "trainingData": "select tokenize(text, {splitChars: ' '}) as * from posneg",
        "outcomes": [["label", "CLICK IS NOT NULL"]],
        "statsTableFileUrl": "file://build/x86_64/tmp/mldb-873-stats_table_posneg.st",
        "runOnCreation": True,
        "functionName": "myBowSt",
        "functionOutcomeToUse": "label"
    }
}
rez = mldb.put("/v1/procedures/myroll_posneg", conf)
mldb.log(rez.json())

conf['params']['outputDataset'] = 'stats_table_counts'
rez = mldb.put("/v1/procedures/myroll_posneg2", conf)
rez = mldb.get('/v1/query', q='select * from stats_table_counts')
mldb.log(rez.json())
assert_for_rows(rez.json(), "I", "trials", 2)
assert_for_rows(rez.json(), "I", "outcome.label", 1)
assert_for_rows(rez.json(), "yellow", "trials", 1)
assert_for_rows(rez.json(), "yellow", "outcome.label", 0)
assert_for_rows(rez.json(), "are", "trials", 3)
assert_for_rows(rez.json(), "are", "outcome.label", 1)

conf = {
    "type": "statsTable.bagOfWords.posneg",
    "params": {
        "numPos": 4,
        "numNeg": 4,
        "minTrials": 1,
        "outcomeToUse": "label",
        "statsTableFileUrl": "file://build/x86_64/tmp/mldb-873-stats_table_posneg.st",
    }
}
rez = mldb.put("/v1/functions/posnegz", conf)
mldb.log(rez.json())

rez = mldb.get(
    "/v1/query",
    q="select posnegz({words: tokenize(text, {splitChars: ' _'})}) as * from posneg")
js_rez = rez.json()
mldb.log(js_rez)

assert_for_rows(js_rez, "d", "probs.red.label", 1)
assert_for_rows(js_rez, "a", "probs.I.label", 0.5)
assert_for_rows(js_rez, "b", "probs.I.label", 0.5)


# lets try with the function we created at procedure run time
rez = mldb.get(
    "/v1/query",
    q="select myBowSt({words: tokenize(text, {splitChars: ' .'})}) as * from posneg")
js_rez = rez.json()
mldb.log(js_rez)

# default min instance is 50 so we should get not columns back
for row in js_rez:
    assert "columns" not in row


mldb.script.set_return("success")
