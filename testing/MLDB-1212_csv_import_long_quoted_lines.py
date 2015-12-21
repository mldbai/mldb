
import json, random, datetime, os

    
with open("tmp/broken_csv.csv", 'wb') as f:
    f.write("a,b\n")
    f.write("1,\"" + " ".join(["word " for x in xrange(50)])+"\"\n")
    f.write("1,\"" + " ".join(["word " for x in xrange(100)])+"\"\n")
    f.write("1,\"" + " ".join(["word " for x in xrange(1000)])+"\"\n")
    f.write("1,\"" + " ".join(["word " for x in xrange(10000)])+"\"\n")
    
result = mldb.perform("PUT", "/v1/datasets/x", [], {
    "type": "text.csv.tabular",
    "params": {
        "dataFileUrl": "file://tmp/broken_csv.csv",
        "ignoreBadLines": False
    }
})
mldb.log(result)

assert result["statusCode"] == 201
jsRez = json.loads(result['response'])
mldb.log(jsRez)

result = mldb.perform("GET", "/v1/query", [["q", "select tokenize(b, {splitchars: ' '}) as cnt from x order by rowName() ASC"]])
jsRez = json.loads(result['response'])
mldb.log(jsRez)

answers = {2: 50, 3: 100, 4: 1000, 5: 10000}
for row in jsRez:
    assert answers[row["rowName"]] == row["columns"][0][1]

mldb.script.set_return("success")

