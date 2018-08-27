# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
from mldb import mldb

rp = request
mldb.log("relpath = " + str(rp.remaining))
mldb.log("verb = " + str(rp.verb))
mldb.log("resource = " + str(rp.resource))
mldb.log("params = " + str(rp.rest_params))
mldb.log("payload = " + str(rp.payload))
mldb.log("contentType = " + str(rp.content_type))
mldb.log("contentLength = " + str(rp.content_length))

import json
mldb.log("headers = " + json.dumps(rp.headers))

print("in route!")

return_code = 200

if rp.remaining == "/emptyList":
    request.set_return([])
elif rp.remaining == "/emptyDict":
    request.set_return({})
elif rp.remaining == "/teaPot":
    request.set_return("tea pot", 418)
else:
    request.set_return({ "how": "are you" })

