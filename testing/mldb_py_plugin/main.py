# -*- coding: utf-8 -*-
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


# test plugin for MLDB

print "testing pluging for MLDB!!"

#global plugin
mldb.log("PYTHON PLUGIN EXECUTIN!!!!")
mldb.log("Mutua Madrileña Madrid Open")
#plugin2 = plugin
pwet = 5

mldb.plugin.serve_static_folder("/static", "static")

