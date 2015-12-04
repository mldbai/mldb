# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

$(eval $(call python_addon,py_conv_test_module,python_converters_test_support.cc,python2.7 boost_python types arch))

$(eval $(call python_test,python_converters_test,py_conv_test_module))
