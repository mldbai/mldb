#
# pytanic_plugin_test.py
# Francois Maillet, 2015-03-26
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#
# Test the pytanic plugin example

import json


pythonScript = {
    "type": "python",
    "params": {
        "address": "git://github.com/datacratic/mldb-pytanic-plugin"
    }
}
resp = mldb.perform("PUT", "/v1/plugins/pytanic", [], pythonScript)

mldb.log(resp)
assert resp['statusCode'] == 201, str(resp)

# did we manage to test the glz?
resp = mldb.perform("GET", "/v1/procedures/titanic_cls_test_glz/runs/1")
mldb.log(resp)
jsResp = json.loads(resp['response'])
assert jsResp["status"]["auc"] > 0.80, "auc upper bound"
assert jsResp["status"]["auc"] < 0.92, "auc lower bound"

######
# let's try to do a predict using the function
### does the function exist?
resp = mldb.perform("GET", "/v1/functions/classifyFunctionglz")
assert resp['statusCode'] == 200, str(resp)

### does the probabilizer function exist
resp = mldb.perform("GET", "/v1/functions/probabilizerglz");
assert resp['statusCode'] == 200, str(resp)

resp = mldb.perform("GET", "/v1/functions/probabilizerglz/application",
                    [ ["input", json.dumps({"features":{
                        "Pclass": "c1", 
                        "Sex": "male", 
                        "Age": 35, 
                        "SibSp": 1, 
                        "Parch": 0, 
                        "Fare": 32, 
                        "Embarked": "C"
                    }})]])

jsResp = json.loads(resp['response'])
mldb.log(jsResp)
score1 = jsResp["output"]["prob"]

assert score1 > 0.40, "score upper bound"
assert score1 < 0.58, "score lower bound"

resp = mldb.perform("GET", "/v1/functions/probabilizerglz/application",
                    [["input", json.dumps({"features":{"Pclass": "c1", 
                                                       "Sex": "female", 
                                                       "Age": 75, 
                                                       "SibSp": 1, 
                                                       "Parch": 0, 
                                                       "Fare": 500, 
                                                       "Embarked": "C"}}) ]])
jsResp = json.loads(resp['response'])
score2 = jsResp["output"]["prob"]

assert score1 < score2, "score ordering"

mldb.script.set_return("success")
