# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

import mylib

print "Handling route in python"

rp = mldb.plugin.rest_params
if rp.verb == "GET" and rp.remaining == "/func":
    mldb.plugin.set_return({"value": mylib.myFunc(mldb)})

mldb.log("nothing!!!")

