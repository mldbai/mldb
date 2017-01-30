# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


rp = mldb.plugin.rest_params
mldb.log("relpath = " + str(rp.remaining))
mldb.log("verb = " + str(rp.verb))
mldb.log("resource = " + str(rp.resource))
mldb.log("params = " + str(rp.rest_params))
mldb.log("payload = " + str(rp.payload))
mldb.log("contentType = " + str(rp.content_type))
mldb.log("contentLength = " + str(rp.content_length))

import json
mldb.log("headers = " + json.dumps(rp.headers))

print "in route!"

if rp.remaining == "/emptyList":
    mldb.plugin.set_return([])
elif rp.remaining == "/emptyDict":
    mldb.plugin.set_return({})
elif rp.remaining == "/teaPot":
    mldb.plugin.set_return("tea pot", 418)
else:
    mldb.plugin.set_return({ "how": "are you" })

