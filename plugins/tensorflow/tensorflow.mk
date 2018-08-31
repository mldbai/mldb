# Makefile for tensorflow plugin for MLDB

ifeq ($(WITH_CUDA),1)
NUM_CUDA_GPUS:=$(shell nvidia-smi -L | grep 'GPU ' | wc -l)
else
NUM_CUDA_GPUS:=0
endif
MANUAL_IF_NO_GPUS:=$(if $(call seq,0,$(NUM_CUDA_GPUS)),manual)

# Tensorflow plugins
LIBMLDB_TENSORFLOW_PLUGIN_SOURCES:= \
	tensorflow_plugin.cc \
    tensorflow_additional_builtin.cc \

$(eval $(call set_compile_option,$(LIBMLDB_TENSORFLOW_PLUGIN_SOURCES),$$(TENSORFLOW_COMPILE_FLAGS) -Imldb/ext/tensorflow))

# Make these depend upon Tensorflow's version of the protobuf compiler
# since the headers that they need are installed with it.
#
# They also need access to the generated tensorflow headers
$(LIBMLDB_TENSORFLOW_PLUGIN_SOURCES:%=$(CWD)/%): $(HOSTBIN)/protoc $(DEPENDS_ON_TENSORFLOW_HEADERS)

$(eval $(call mldb_plugin_library,tensorflow,mldb_tensorflow_plugin,$(LIBMLDB_TENSORFLOW_PLUGIN_SOURCES),tensorflow-cpp-interface))

$(eval $(call mldb_builtin_plugin,tensorflow,mldb_tensorflow_plugin,doc))

$(eval $(call mldb_unit_test,MLDB-1203-tensorflow-plugin.js,tensorflow,,,{"GPUS": $(NUM_CUDA_GPUS)}))
$(eval $(call mldb_unit_test,MLDB-1736-tensorflow-builtins.js,tensorflow))


#$(eval $(call include_sub_make,pro_testing,testing,pro_testing.mk))
