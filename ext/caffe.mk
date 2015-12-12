ifneq ($(PREMAKE),1)

PROTOC ?= protoc

CAFFE_FILES_FULLPATH := $(shell find $(CWD) -name "*.cpp" | grep -v examples | grep -v test | grep -v python | grep -v matlab | grep -v tools)
CAFFE_FILES := $(CAFFE_FILES_FULLPATH:$(CWD)/%=%)

#$(warning CAFFE_FILES=$(CAFFE_FILES))

$(CAFFE_FILES_FULLPATH): $(CWD)/src/caffe/proto/caffe.pb.h

$(eval $(call set_compile_option,$(CAFFE_FILES),-I$(CWD)/src/ -I$(CWD)/include -I$(CWD)/ext/caffe/src -DCPU_ONLY))

$(eval $(call library,caffe,$(CAFFE_FILES) src/caffe/proto/caffe.pb.cc,protobuf glog gflags boost_system boost_filesystem m hdf5_hl hdf5))

$(CWD)/src/caffe/proto/caffe.pb.h $(CWD)/src/caffe/proto/caffe.pb.cc: $(CWD)/src/caffe/proto/caffe.proto
	$(PROTOC) ext/caffe/src/caffe/proto/caffe.proto --cpp_out=.

endif
