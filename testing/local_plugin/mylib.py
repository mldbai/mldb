# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

def myFunc(mldb):
    mldb.log(mldb.plugin.get_plugin_dir())
    return "in function"

