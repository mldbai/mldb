# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
from mldb import mldb
import mylib

print("Handling route in python")

rp = request
if rp.verb == "GET" and rp.remaining == "/func":
    request.set_return({"value": mylib.myFunc(mldb)})

mldb.log("nothing!!!")

