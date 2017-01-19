# This file is part of MLDB. Copyright 2015 mldb.ai inc All rights reserved.


verb = mldb.plugin.rest_params.verb
remaining = mldb.plugin.rest_params.remaining

mldb.log(str(verb))
mldb.log(str(remaining))
mldb.log("handling request " + verb + " " + remaining)

if "pathExists" in remaining:
    mldb.plugin.set_return({"val":"I'm feeling good!"})
else:
    raise Exception("Exception in handleRequest thrown on purpose for testing")

