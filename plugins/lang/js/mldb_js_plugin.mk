# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.



V8_ARCH_x86_64:=x64
V8_ARCH_aarch64:=arm64
V8_ARCH_arm:=arm

V8_ARCH:=$(V8_ARCH_$(ARCH))

$(if $(V8_ARCH),,$(error couldnt find v8 architecture for $(ARCH)))

$(LIB)/libv8$(so): mldb/ext/v8-cross-build-output/$(V8_ARCH)/libv8.so
	@cp $< $@~ && mv $@~ $@

ifneq ($(PREMAKE),1)

$(LIB)/natives_blob.bin: mldb/ext/v8-cross-build-output/$(V8_ARCH)/natives_blob.bin
	@cp $< $@~ && mv $@~ $@

$(LIB)/snapshot_blob.bin: mldb/ext/v8-cross-build-output/$(V8_ARCH)/snapshot_blob.bin
	@cp $< $@~ && mv $@~ $@

endif

LIB_v8_DEPS := $(LIB)/libv8$(so) $(LIB)/natives_blob.bin $(LIB)/snapshot_blob.bin

LIBJS_LINK := jsoncpp v8 arch utils types

# JS plugin loader

JS_PLUGIN_SOURCE := \
	js_utils.cc \
	js_value.cc \
	js_loader.cc \
	js_function.cc \
	js_common.cc \
	dataset_js.cc \
	function_js.cc \
	procedure_js.cc \
	mldb_js.cc \
	sensor_js.cc \

$(eval $(call set_compile_option,$(JS_PLUGIN_SOURCE),-Imldb/ext/v8-cross-build-output/include))

$(eval $(call library,mldb_js_plugin,$(JS_PLUGIN_SOURCE),value_description $(LIBJS_LINK) sql_expression))
