# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,python_interpreter_test,python_interpreter $(PYTHON_LIBRARY) nanobind arch base value_description types,boost))

$(eval $(call set_compile_option,python_interpreter_test.cc,-I$(PYTHON_INCLUDE_PATH) $(NANOBIND_COMPILE_OPTIONS)))
