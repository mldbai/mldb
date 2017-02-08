# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

import mylib

mldb.log("Running from a gist!")

mldb.plugin.serve_documentation_folder("doc")

# MLDBFB-451
mldb = mldb_wrapper.wrap(mldb)
