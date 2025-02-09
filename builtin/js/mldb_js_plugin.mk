# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

USE_PLATFORM_V8 ?= 1

ifeq ($(USE_PLATFORM_V8),1)

V8_INCLUDE_PATH?=/usr/include/v8

else
V8_RELEASE_CODENAME?=$(if $(DISTRIB_CODENAME),$(DISTRIB_CODENAME),$(error No distribution codename found; does /etc/lsb-release exist and define DISTRIB_CODENAME?))

V8_ARCH_x86_64:=x64
V8_ARCH_aarch64:=arm64
V8_ARCH_arm:=arm

V8_ARCH:=$(V8_ARCH_$(ARCH))

$(if $(V8_ARCH),,$(error couldnt find v8 architecture for $(ARCH); set V8_ARCH_$(ARCH) in mldb_js_plugin.mk))

$(LIB)/libv8$(so): mldb/ext/v8-cross-build-output/$(V8_RELEASE_CODENAME)/$(V8_ARCH)/libv8$(SO_EXTENSION)
	@cp $< $@~ && mv $@~ $@

ifneq ($(PREMAKE),1)

$(LIB)/natives_blob.bin: mldb/ext/v8-cross-build-output/$(V8_RELEASE_CODENAME)/$(V8_ARCH)/natives_blob.bin
	@cp $< $@~ && mv $@~ $@

$(LIB)/snapshot_blob.bin: mldb/ext/v8-cross-build-output/$(V8_RELEASE_CODENAME)/$(V8_ARCH)/snapshot_blob.bin
	@cp $< $@~ && mv $@~ $@

endif

LIB_v8_DEPS := $(LIB)/libv8$(so) $(LIB)/natives_blob.bin $(LIB)/snapshot_blob.bin
V8_INCLUDE_PATH:=mldb/ext/v8-cross-build-output/include

endif


LIBJS_LINK := \
	value_description \
	v8 \
	v8_libplatform \
	arch \
	utils \
	types \
	mldb_engine \
	log \
	any \
	mldb_core \
	rest \
	rest_entity \
	json_diff \
	logging \
	vfs \
	base \
	mldb_core \
	mldb_builtin_base \
	sql_types \
	http

# JS plugin loader

JS_PLUGIN_SOURCE := \
	js_utils.cc \
	js_value.cc \
	js_plugin.cc \
	js_function.cc \
	js_common.cc \
	dataset_js.cc \
	function_js.cc \
	procedure_js.cc \
	mldb_js.cc \
	sensor_js.cc \
	mldb_v8_platform_common.cc \
	mldb_v8_platform_v11.cc \
	mldb_v8_platform_v12.cc \


$(eval $(call set_compile_option,$(JS_PLUGIN_SOURCE),-I$(V8_INCLUDE_PATH) $(V8_INCLUDE_FLAGS)))

$(eval $(call library,mldb_js_plugin,$(JS_PLUGIN_SOURCE),value_description $(LIBJS_LINK) sql_expression))
