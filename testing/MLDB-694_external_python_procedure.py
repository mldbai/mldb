#
# MLDB-694_external_python_procedure.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa


conf = {
    "type": "experimental.external.procedure",
    "params": {
        "stdInData": "pwet",
        "scriptConfig": {
            "source": """
import sys
import select
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

def fib(n):
    a,b = 1,1
    for i in range(n-1):
        a,b = b,a+b
    return a

print json.dumps({"bouya": 5, "stdin_data": std_in})
"""
        }
    }
}

rez = mldb.put("/v1/procedures/externalProc", conf)
mldb.log(rez.text)

rez = mldb.put("/v1/procedures/externalProc/runs/1")
mldb.log(rez.text)

js_resp = rez.json()
mldb.log(js_resp)

assert js_resp["status"]["return"] == {"bouya": 5, "stdin_data" : "pwet"}

mldb.script.set_return("success")
