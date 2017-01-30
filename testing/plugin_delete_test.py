# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.



pythonScript = {
    "type": "python",
    "params": {
        "address": "",
        "source": """

mldb.log("Constructing plugin!")


def requestHandler(mldb, remaining, verb, resource, restParams, payload, contentType, contentLength, headers):
    mldb.log("waqlsajf;lasdf")
    print "Handling route in python"

    if verb == "GET" and remaining == "/miRoute":
        return "bouya!"

mldb.plugin.set_request_handler(requestHandler)

"""
    }
}


mldb.script.set_return("hoho")
mldb.log("ouin")

mldb.log(str(mldb.perform("PUT", "/v1/plugins/plugToDel", [["sync", "true"]], pythonScript)))

mldb.log(str(mldb.perform("GET", "/v1/plugins", [], {})))

rtn= mldb.perform("GET", "/v1/plugins/plugToDel/routes/miRoute", [], {})
print rtn

mldb.script.set_return(rtn["response"])

