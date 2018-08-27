#
# MLDB-153_pyscript_state_pollution.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
from mldb import mldb, ResponseException

conf = {
    "source": "a=1"
}
rtn = mldb.post("/v1/types/plugins/python/routes/run", conf)
mldb.log(rtn.text)

conf = {
    "source": "print(a)"
}

try:
    mldb.post("/v1/types/plugins/python/routes/run", conf)
except ResponseException as exc:
    rtn = exc.response
else:
    assert False, 'should not be here'

mldb.log(rtn.text)
js_rtn = rtn.json()
mldb.log(js_rtn)

assert js_rtn["exception"]["message"] == "name 'a' is not defined"

request.set_return("success")
