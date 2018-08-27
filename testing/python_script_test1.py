# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

from mldb import mldb

# Test python script

print("Hello, world!")

print(mldb.script)

mldb.log(str(mldb.script))
mldb.log("Hello, world")
