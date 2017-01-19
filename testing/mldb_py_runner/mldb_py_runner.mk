# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call python_module,mldb_py_runner, __init__.py mldb_py_runner.py,))

$(BIN)/mldb_py_runner/mldb_py_runner.py: $(BIN)/mldb_runner
