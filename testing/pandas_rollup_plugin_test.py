# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


import os, socket, time
#import csv
#import json
#import datetime
from multiprocessing import Process
from py_connectors.mldb_connector import MldbConnectorAdHoc
#from mldb.data import DFrame

def startMldb():
    s = socket.socket()
    s.bind(('', 0))
    port = s.getsockname()[1]
    s.close()

    def startServer(port):
        os.system("build/x86_64/bin/mldb_runner --http-listen-port %d --peer-listen-port %d" % (port, port+1))

    proc = Process(target=startServer, args=[port])
    proc.start()
    time.sleep(1)     # give enough time to start
    if not proc.is_alive():
        raise Exception("Failed to start api in background for test")
    return proc, "http://localhost:%d" % port

mldbProc, mldbUrl = startMldb()

mldb = MldbConnectorAdHoc(mldbUrl).v1()

#######
# First we need to register the two plugins we'll be using
#   b) the pandas_rollup plugin, which we'll use to do our exploration
######

pluginConfig = {
        "type": "pandas_rollup",
        "id": "pandas_rollup"
}
print mldb.plugins("pandas_rollup").put(pluginConfig, [("sync", "true")])



####
# Let's now create a script that we'll ship over and that will be executed
#  on the server to create the dataset and import the data
####

scriptSource = """
import json
from datetime import datetime

print "Running a server-side script!!!"

# create a mutable beh dataset
datasetConfig = {
    "type": "beh.mutable",
    "id": "tng",
    "address": "tng_py.beh.gz"
}

dataset = plugin.create_dataset(datasetConfig)

dataset.recordRow("picard", [["setscourse", "earth", datetime.fromtimestamp(1000)],
                             ["setscourse", "neutralzone", datetime.fromtimestamp((10000))],
                             ["setscourse", "neutralzone", datetime.fromtimestamp((20000))],
                             ["setscourse", "neutralzone", datetime.fromtimestamp((30000))],
                             ["setscourse", "neutralzone", datetime.fromtimestamp((4000))],
                             ["setscourse", "wolf359", datetime.fromtimestamp((50000))]])
dataset.recordRow("riker", [["setscourse", "risa", datetime.fromtimestamp((500000))],
                             ["fireon", "cardasians", datetime.fromtimestamp((500000))]])
dataset.recordRow("worf", [["fireon", "tardis", datetime.fromtimestamp((400000))],
                           ["fireon", "borgcube", datetime.fromtimestamp((500000))],
                           ["fireon", "cardasians", datetime.fromtimestamp((300000))]])
dataset.recordRow('One Zero', [["work", 1, datetime.fromtimestamp((300000))],
                               ["sleep", 0, datetime.fromtimestamp((300000))]])
dataset.recordRow('Double', [["work", 1.5, datetime.fromtimestamp((300000))],
                               ["sleep", 0.4, datetime.fromtimestamp((300000))]])
"""

# post the script for execution on the server
scriptConfig = {
        "scriptSource": scriptSource
        }
print MldbConnectorAdHoc(mldbUrl)._post("/v1/types/plugins/python/routes/run", "")(scriptConfig)

import json
queryConfig = {
        "dataset": "tng",
        "query": json.dumps({"head": "groupby", "body": [], "tail":["list_both"]})
}

print MldbConnectorAdHoc(mldbUrl)._post("/v1/plugins/pandas_rollup/routes/query", "")(queryConfig)

print queryConfig
# /v1/plugins/pandas_rollup/routes/query
# DFrame

