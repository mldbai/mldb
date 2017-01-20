# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

#####
#   Testing if the error responses are all formatted in the same way
#   Guy Dumais, January 27, 2016
#   Copyright mldb.ai inc 2016
####

import json, random, datetime

rez = mldb.perform("GET", "/v1/query", [['q', 'sele']], {})
mldb.log(rez)
response = json.loads(rez['response'])
mldb.log(response)
assert 'error' in response, 'expected an error message in the response'
assert 'httpCode' in response, 'expected an http code in the response'

rez = mldb.perform("GET", "/v1/query", [['q', 'select h()']], {})
mldb.log(rez)
response = json.loads(rez['response'])
mldb.log(response)
assert 'error' in response, 'expected an error message in the response'
assert 'httpCode' in response, 'expected an http code in the response'

rez = mldb.perform("GET", "/v1/querry", [], {})
mldb.log(rez)
response = json.loads(rez['response'])
mldb.log(response)
assert 'error' in response, 'expected an error message in the response'
assert 'httpCode' in response, 'expected an http code in the response'

mldb.script.set_return("success")
