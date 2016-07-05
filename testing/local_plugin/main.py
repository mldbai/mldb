# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import mylib

mldb.log("Running from a gist!")

mldb.get("/v1/dataset")

mldb.plugin.serve_documentation_folder("doc")

