# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


endpoint = mldb.plugin.args[0]
folder = mldb.plugin.args[1]

mldb.log("Trying with enpoint '%s' -> '%s'" % (endpoint, folder))

mldb.plugin.serve_static_folder(endpoint, folder)

