#
# python_argv_test.py
# Mich, 2016-01-15
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

import sys
sys.argv

if False:
    mldb = None

mldb.log(str(sys.argv))
mldb.script.set_return("success")
