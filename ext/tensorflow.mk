# tensorflow.mk
# Jeremy Barnes, 17 December 2015
# Copyright (c) 2015 Datacratic Inc.
#
# This file is part of mldb.ai, Apache 2.0 license.
#
# This is an alternative make-based build of Tensorflow, that operates on
# unmodified Tensorflow source code.  It uses standard GNU Make, with some
# functionality from jml-build, instead of bazel.
#
# Bazel is a great build system, and is certainly the right thing to use
# when developing tensorflow due to the complex dependencies and productivity
# gains of having perfect builds, but its requirements are high for someone
# who merely wants to use tensorflow packaged within a larger system.  Hence
# this alternative, parallel build script which is just enough to get it into
# mldb.ai.
#
# Note that we're not trying to build tensorflow standalone, but as part
# of mldb.ai, so we don't deal with many of the nice extras like tensorboard,
# the Python APIs, etc.

ifneq ($(PREMAKE),1)

# Copy the current working directory, which can't be used inside a rule since it's
# expanded once the rule runs, not when it's instantiated
TF_CWD:=$(CWD)

# Tensorflow needs version 3 of protobuf, which it bundles via a submodule.
# We start by building that.  Note that we need to compile it to run on the
# HOST, as it's a build dependency, not on the target.
#
# We use a recursive make, since we don't care about anything apart from the
# final executable.
$(HOSTBIN)/protoc:
	cd mldb/ext/tensorflow/google/protobuf && ./autogen.sh && ./configure --prefix $(PWD)/$(BUILD)/$(HOSTARCH) && $(MAKE) -j && $(MAKE) -j install

# Now we have it, we use it to compile all of the .proto files in the TensorFlow
# source directory.  Here we find all of those files
TENSORFLOW_PROTOBUF_FILES:=$(shell find $(CWD)/tensorflow/core -name "*.proto")
TENSORFLOW_PROTOBUF_BUILD:=$(TENSORFLOW_PROTOBUF_FILES:$(CWD)/%.proto=%.pb.cc)

# Find all of the .cc files that we need to bundle up to run the tensorflow
# kernels.  For the moment, we don't use any Cuda constructs, so we strip those
# out.
TENSORFLOW_CC_FILES:=$(shell (find $(CWD)/tensorflow/core -name "*.cc"; find $(CWD)/tensorflow/stream_executor -name "*.cc") | grep -v '\.cu\.cc' | grep -v '\.pb\.cc' | grep -v '.*_ops\.cc' | grep -v 'ops/no_op.cc' | grep -v test | grep -v cuda | grep -v tutorial | grep -v user_ops)
TENSORFLOW_CC_FILES+=$(shell find $(CWD)/tensorflow/core/kernels -name "*.cc" | grep -v test | grep -v '\.cu\.cc' | grep -v '\.pb\.cc')

# Turn them into relative pathnames that our rule system can understand to turn
# it into a library.
TENSORFLOW_CC_BUILD:=$(sort $(TENSORFLOW_CC_FILES:$(CWD)/%=%))

# Tensorflow comes bundled with scripts that download various external things
# that we already have as a system dependency, and hardcodes the include
# path.  We create include directories that forward to /usr/include to point
# to the system versions.
TENSORFLOW_INCLUDES:= \
	$(INC)/external/png_archive/libpng-1.2.53

# How to actually make an include directory pointing to /usr/include
$(TENSORFLOW_INCLUDES):
	mkdir -p $(dir $(@)) && ln -s /usr/include $(@)

# We need the 9a release of libjpeg, which is incldued in ext.  Here we set
# up the include path so it can be found.
$(INC)/external/jpeg_archive/jpeg-9a:
	mkdir -p $(dir $(@)) && ln -s $(PWD)/mldb/ext/jpeg $(@)

# It also needs re2, which we have locally in ext.  We create the necessary
# include directories.
$(INC)/external/re2:
	mkdir -p $(dir $(@)) && ln -s $(PWD)/mldb/ext/re2 $(@)

# Here is the list of files we need to compile for tensorflow to be incorporated
$(TENSORFLOW_CC_FILES):	$(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.cc) | $(TENSORFLOW_INCLUDES) $(INC)/external/re2 $(INC)/external/jpeg_archive/jpeg-9a

# To create a protobuf file, we compile the input file
$(CWD)/%.pb.cc:		$(CWD)/%.proto $(HOSTBIN)/protoc
	$(HOSTBIN)/protoc $< -Imldb/ext/tensorflow --cpp_out=$(TF_CWD)

# Here are the flags that anything that includes TensorFlow needs to
# define.
TENSORFLOW_WARNING_FLAGS := -Wno-reorder -Wno-return-type -Wno-overflow -Wno-overloaded-virtual -Wno-parentheses -Wno-maybe-uninitialized -Wno-array-bounds
TENSORFLOW_INCLUDE_FLAGS := -I$(CWD) -I$(INC) -I$(CWD)/third_party/eigen3 -Imldb/ext/re2
TENSORFLOW_COMPILE_FLAGS := $(TENSORFLOW_WARNING_FLAGS) $(TENSORFLOW_INCLUDE_FLAGS)

# We need to set up include paths and turn off a bunch of warnings.  Most of them
# are triggered by the (3rd party) Eigen library.
$(eval $(call set_compile_option,$(TENSORFLOW_CC_BUILD) $(TENSORFLOW_PROTOBUF_BUILD),$(TENSORFLOW_COMPILE_FLAGS)))

TENSORFLOW_LINK := protobuf re2 png jpeg

# Finally, build a library with the tensorflow functionality inside
$(eval $(call library,tensorflow,$(TENSORFLOW_CC_BUILD) $(TENSORFLOW_PROTOBUF_BUILD),$(TENSORFLOW_LINK)))

# And create a nice target name
tensorflow_lib: $(LIB)/libtensorflow.so

# Part of the C++ interface to TensorFlow is created using an automatic
# generator.  First we build the actual program, which links to the basic
# set of kernels

TENSORFLOW_CC_OP_GEN_FILES := \
	tensorflow/cc/ops/cc_op_gen.cc \
	tensorflow/cc/ops/cc_op_gen_main.cc

$(eval $(call set_compile_option,$(TENSORFLOW_CC_OP_GEN_FILES),$(TENSORFLOW_COMPILE_FLAGS)))

$(eval $(call program,cc_op_gen,tensorflow,$(TENSORFLOW_CC_OP_GEN_FILES)))


# Now we have that program, we link it to one set of operations at a time and
# generate the files.  First, though, we need to compile each of those as a
# separate module

TENSORFLOW_OPS_SOURCES := $(shell find $(CWD)/tensorflow/core/ops -name "*_ops.cc")
TENSORFLOW_OPS := $(TENSORFLOW_OPS_SOURCES:$(CWD)/tensorflow/core/ops/%_ops.cc=%)

# Create a library for each of the ops.  We also have to adjust the compilation
# flags so that it actually compiles
$(foreach op,$(TENSORFLOW_OPS), \
	$(eval $(call set_compile_option,tensorflow/core/ops/$(op)_ops.cc,$(TENSORFLOW_COMPILE_FLAGS))) \
	$(eval $(call library,tensorflow_$(op)_ops,tensorflow/core/ops/$(op)_ops.cc,tensorflow)) \
)

# Put them all into one convenient library, along with the no_op.  The no_op
# has to be here, and not in the core library, as otherwise the interface is
# generated for each of the ops and we have multiple definitions.
$(eval $(call set_compile_option,tensorflow/core/ops/no_op.cc,$(TENSORFLOW_COMPILE_FLAGS)))
$(eval $(call library,tensorflow_ops,tensorflow/core/ops/no_op.cc,$(foreach op,$(TENSORFLOW_OPS),tensorflow_$(op)_ops)))

# Rule to auto-generate source code for the c interface.  We preload the operations
# library and then run the tool to generate the code.
#
# The user_ops.h header is used as a placeholder for those who want to extend
# tensorflow in-tree.  Since MLDB provides other means for extension, and we
# don't want to modify the source tree, we put a dummy, empty header in there
# to signify that there are no user operations available.
$(CWD)/tensorflow/cc/ops/%_ops.cc:	$(BIN)/cc_op_gen $(LIB)/libtensorflow_%_ops.so
	LD_PRELOAD=$(LIB)/libtensorflow_$(*)_ops.so $(BIN)/cc_op_gen $(TF_CWD)/tensorflow/cc/ops/$(*)_ops.h $(TF_CWD)/tensorflow/cc/ops/$(*)_ops.cc 0
	touch $(TF_CWD)/tensorflow/cc/ops/user_ops.h

# Find the source files needed for the C++ interface.  Some are pre-packaged and
# others were generated above.
TENSORFLOW_CC_INTERFACE_FILES:=$(sort $(shell (find $(CWD)/tensorflow/cc -name "*.cc" | grep -v example)) $(foreach op,$(TENSORFLOW_OPS),$(CWD)/tensorflow/cc/ops/$(op)_ops.cc))

TENSORFLOW_CC_INTERFACE_BUILD:=$(TENSORFLOW_CC_INTERFACE_FILES:$(CWD)/%=%)

# Create the interface library
$(eval $(call set_compile_option,$(TENSORFLOW_CC_INTERFACE_BUILD),$(TENSORFLOW_COMPILE_FLAGS)))
$(eval $(call library,tensorflow_cpp_interface,$(TENSORFLOW_CC_INTERFACE_BUILD),tensorflow_ops))

endif
