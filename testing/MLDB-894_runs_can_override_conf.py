# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import json

conf = {
    "type": "experimental.external.procedure",
    "params": {
        "stdInData": "pwet",
        "scriptConfig": {
            "source": """
import sys
import select
import numpy
import json

std_in = ""

# If there's input ready, do something, else do something
# else. Note timeout is zero so select won't block at all.
# https://repolinux.wordpress.com/2012/10/09/non-blocking-read-from-stdin-in-python/
while sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
    line = sys.stdin.readline()
    if line:
        std_in += line
    else: # an empty line means stdin has been closed
        break

print json.dumps({"stdin_data": std_in})
"""
        }
    }
}
   
rez = mldb.perform("PUT", "/v1/procedures/externalProc", [], conf)
mldb.log(rez)    
assert rez["statusCode"] == 201

rez = mldb.perform("PUT", "/v1/procedures/externalProc/runs/1")
mldb.log(rez)
assert rez["statusCode"] == 201

jsResp = json.loads(rez["response"])
mldb.log(jsResp)
assert jsResp["status"]["return"] == {"stdin_data" : "pwet"}



rez = mldb.perform("PUT", "/v1/procedures/externalProc/runs/2", [], {"params": {"stdInData": "I CHANGED IT!!"}})
mldb.log(rez)
assert rez["statusCode"] == 201

jsResp = json.loads(rez["response"])
mldb.log(jsResp)
assert jsResp["status"]["return"] == {"stdin_data" : "I CHANGED IT!!"}

mldb.script.set_return("success")
