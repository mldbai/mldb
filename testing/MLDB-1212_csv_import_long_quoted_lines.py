
import json, random, datetime, os

    
with open("tmp/broken_csv.csv", 'wb') as f:
    f.write("a,b\n")
    f.write("1,\"" + " ".join(["word " for x in xrange(50)])+"\"\n")
    f.write("1,\"" + " ".join(["word " for x in xrange(100)])+"\"\n")
    f.write("1,\"" + " ".join(["word " for x in xrange(1000)])+"\"\n")
    
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

mldb.script.set_return("success")

