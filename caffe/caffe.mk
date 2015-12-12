# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
# Makefile for caffe plugin for MLDB

# Caffe plugins
LIBMLDB_CAFFE_PLUGIN_SOURCES:= \
	caffe_plugin.cc \
	caffe_train.cc \
	caffe_predict.cc

$(eval $(call set_compile_option,$(LIBMLDB_CAFFE_PLUGIN_SOURCES),-I../caffe/include -I../caffe/build/src -DCPU_ONLY))

$(eval $(call mldb_plugin_library,caffe,mldb_caffe_plugin,$(LIBMLDB_CAFFE_PLUGIN_SOURCES),ml protobuf caffe,-L../caffe/build/lib))

$(eval $(call mldb_builtin_plugin,caffe,mldb_caffe_plugin,doc))

$(eval $(call include_sub_make,caffe_testing,testing,caffe_testing.mk))
