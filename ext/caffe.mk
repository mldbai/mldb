ifneq ($(PREMAKE),1)

PROTOC ?= protoc

CAFFE_FILES_FULLPATH := $(shell find $(CWD) -name "*.cpp" | grep -v examples | grep -v test | grep -v python | grep -v matlab | grep -v tools)
CAFFE_FILES := $(CAFFE_FILES_FULLPATH:$(CWD)/%=%)

$(warning CAFFE_FILES=$(CAFFE_FILES))

$(CAFFE_FILES_FULLPATH): $(BUILD)/tmp/include/caffe/proto/caffe.pb.h

$(eval $(call set_compile_option,$(CAFFE_FILES),-I$(CWD)/src/ -I$(CWD)/include -I$(BUILD)/tmp/include -DCPU_ONLY))

$(eval $(call library,caffe,$(CAFFE_FILES) ,protobuf))

$(BUILD)/tmp/include/caffe/proto/caffe.pb.h $(BUILD)/tmp/include/caffe/proto/caffe.pb.cc: $(CWD)/src/caffe/proto/caffe.proto
	mkdir -p $(BUILD)/tmp/include/
	cd ext/caffe/src && $(PROTOC) caffe/proto/caffe.proto --cpp_out=../../../$(BUILD)/tmp/include/

endif
