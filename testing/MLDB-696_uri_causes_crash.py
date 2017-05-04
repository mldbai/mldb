# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


try:
    mldb.create_dataset({
            "type": "beh.mutable", "id": "x",
            "params":{
                "dataFileUrl": "relative/path/without/protocol.beh"
                }}).commit() #should complain about missing protocol!

    mldb.script.set_return("failure")
    exit()
except:
    pass

try:
    mldb.create_dataset({
            "type": "beh.mutable", "id": "y",
            "params":{
                "dataFileUrl": "/asbolute/path/without/protocol.beh"
                }}).commit() #should complain about missing protocol!
    mldb.script.set_return("failure")
    exit()
except:
    pass


mldb.script.set_return("success")

