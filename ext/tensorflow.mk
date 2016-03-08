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

comma=,

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
	@(cd mldb/ext/tensorflow/google/protobuf && ./autogen.sh && ./configure --prefix $(PWD)/$(BUILD)/$(HOSTARCH) && $(MAKE) -j && $(MAKE) -j install) > $(TMP)/protoc-build.log || (echo "protobuf build failed" && cat $(TMP)/protoc-build.log && false)

# We use the protobuf library at runtime, so it needs to be installed in
# the right place.  Here we also rename it to avoid clashes with the system
# version.
ifneq ($(LIB),$(HOSTLIB))
$(LIB)/libprotobuf3.so:	$(HOSTBIN)/protoc
	@cp $(HOSTLIB)/libprotobuf.so $@
	@cp $(HOSTLIB)/libprotobuf.so.* $(LIB)
else
$(LIB)/libprotobuf3.so:	$(HOSTBIN)/protoc
	@cp $(HOSTLIB)/libprotobuf.so $@
endif

# We also need to make sure that the header files go in the right place
ifneq ($(INC),$(HOSTINC))
$(INC)/google/protobuf:	$(HOSTBIN)/protoc
	@mkdir -p $(INC)/google && ln -sf $(PWD)/$(HOSTINC)/google/protobuf $@
else
$(INC)/google/protobuf: $(HOSTBIN)/protoc
endif

# Now we have it, we use it to compile all of the .proto files in the TensorFlow
# source directory.  Here we find all of those files
TENSORFLOW_PROTOBUF_FILES:=$(shell find $(CWD)/tensorflow/core -name "*.proto")
TENSORFLOW_PROTOBUF_BUILD:=$(TENSORFLOW_PROTOBUF_FILES:$(CWD)/%.proto=%.pb.cc)

# Find all of the .cc files that we need to bundle up to run the tensorflow
# kernels.  We strip out any Cuda constructs, and add them back in later in
# the part of the makefile designed around Cuda support.

TENSORFLOW_CC_FILES:=$(shell (find $(CWD)/tensorflow/core -name "*.cc"; find $(CWD)/tensorflow/stream_executor -name "*.cc") | grep -v '\.cu\.cc' | grep -v '\.pb\.cc' | grep -v '.*_ops\.cc' | grep -v 'ops/no_op.cc' | grep -v test | grep -v tutorial | grep -v user_ops | grep -v 'fact_op\.cc' $(if $(WITH_CUDA),, | grep -v cuda) | grep -v cuda_dnn | grep -v /kernels/)

# Tensorflow comes bundled with scripts that download various external things
# that we already have as a system dependency, and hardcodes the include
# path.  We create include directories that forward to /usr/include to point
# to the system versions.
TENSORFLOW_INCLUDES:= \
	$(INC)/external/png_archive/libpng-1.2.53

# How to actually make an include directory pointing to /usr/include
$(TENSORFLOW_INCLUDES):
	@mkdir -p $(dir $(@)) && ln -s /usr/include $(@)

# We need the 9a release of libjpeg, which is incldued in ext.  Here we set
# up the include path so it can be found.
$(INC)/external/jpeg_archive/jpeg-9a: $(JPEG_INCLUDE_FILES)
	@mkdir -p $(dir $(@)) && ln -s $(PWD)/mldb/ext/jpeg $(@)

# It also needs re2, which we have locally in ext.  We create the necessary
# include directories.
$(INC)/external/re2:
	@mkdir -p $(dir $(@)) && ln -s $(PWD)/mldb/ext/re2 $(@)

# Finally, we need eigen3.  It includes it with the specific Mercurial
# hash needed, which we list here.  This is also included in ext.
TENSORFLOW_EIGEN_MERCURIAL_HASH:=$(shell grep 'archive_dir' mldb/ext/tensorflow/eigen.BUILD | head -n1 | sed 's/.*"eigen-eigen-\(.*\)".*/\1/')
$(if $(TENSORFLOW_EIGEN_MERCURIAL_HASH),,$(error Couldnt find Eigen hash.  You may need to run `git submodule update --init --recursive` to get all the required submodules.))
$(INC)/external/eigen_archive/eigen-eigen-$(TENSORFLOW_EIGEN_MERCURIAL_HASH):
	@mkdir -p $(dir $(@)) && ln -sf $(PWD)/mldb/ext/eigen $(@)

# To create a protobuf file, we compile the input file.  Same for the .h
# file.
$(CWD)/%.pb.cc $(CWD)/%.pb.h:		$(CWD)/%.proto $(HOSTBIN)/protoc
	@echo "         $(COLOR_CYAN)[PROTO]$(COLOR_RESET)			$(basename $<)"
	@$(HOSTBIN)/protoc $< -Imldb/ext/tensorflow --cpp_out=$(TF_CWD)

# Flags required to include the eigen linear algebra framework.  This
# has moved around a fair bit over the Tensorflow releases.
 TENSORFLOW_EIGEN_INCLUDES:=-Imldb/ext/eigen -I$(CWD)/third_party/eigen3

# Include flags for all of Tensorflow
TENSORFLOW_BASE_INCLUDE_FLAGS := -I$(CWD) -I$(INC) -Imldb/ext/re2 $(TENSORFLOW_EIGEN_INCLUDES)

# This part deals with CUDA support.  It's only enabled if WITH_CUDA is
# equal to 1.
ifeq ($(WITH_CUDA),1)

# This is the directory that the whole CUDA development kit is installed
# inside.
CUDA_BASE_DIR?=/usr/local/cuda-7.5

# This is the directory that the CUDA header files are included in.
# If it's not passed into the makefile
CUDA_SYSTEM_HEADER_DIR?=$(CUDA_BASE_DIR)/include

# The nvcc compiler, used to compile .cu.cc files.
NVCC?=$(CUDA_BASE_DIR)/bin/nvcc

# These are the CUDA header files that are used from within Tensorflow
# and expected to live under the third_party/gpus/cuda/include directory.
# We link them individually in the directory that they're expected in
# to avoid issues with header name clashes.

CUDA_HEADERS := cuda.h cublas.h cufft.h cuComplex.h vector_types.h builtin_types.h device_types.h host_defines.h driver_types.h surface_types.h texture_types.h cublas_v2.h curand.h cuda_runtime.h cublas_api.h cuda_fp16.h

# If we are using CUDA, we also need to link its include directory into
# third_party/gpus/cuda/include

$(INC)/third_party/gpus/cuda/include/%: $(CUDA_SYSTEM_HEADER_DIR)/%
	@mkdir -p $(dir $@)
	@ln -s $< $@

# Anything that's to do with Cuda depends on these header files, so set
# up the dependency.

$(TENSORFLOW_CC_FILES): \
	$(foreach header,$(CUDA_HEADERS),$(INC)/third_party/gpus/cuda/include/$(header))

# Some of the CUDA headers include their header dependencies via <file.h>
# instead of "file.h", which requires us to set up the system header directory
# to find them in.
TENSORFLOW_CUDA_INCLUDE_FLAGS:=-Isystem$(CUDA_SYSTEM_HEADER_DIR) -I$(CUDA_SYSTEM_HEADER_DIR)

# Now we find all of the cuda files that need to be compiled by nvidia's
# nvcc compiler.  These are anything that end in .cu.cc
TENSORFLOW_CUDA_NVCC_FILES:=$(shell (find $(CWD)/tensorflow -name "*.cu.cc"))

# Turn them into the symbolic source names we need for the build system
TENSORFLOW_CUDA_NVCC_BUILD:=$(sort $(TENSORFLOW_CUDA_NVCC_FILES:$(CWD)/%=%))

# Here are the common flags to compile tensorflow with CUDA support.
# The whole of Tensorflow, not just the CUDA kernels, need to have these
# set.  They are:
#
# - Including the CUDA header file path in the include search directory,
#   so that the headers can be found.
# - GOOGLE_CUDA=1 is required for Tensorflow to build its CUDA support.
#   Otherwise every file will compile, but those that require CUDA will
#   not produce any symbols (the whole file will be #ifdef'd out).
# - EIGEN_AVOID_STL_ARRAY is required to have the same function signature
#   between the CUDA and non-CUDA kernels; otherwise there will be linker
#   errors.  If you get something like "undefined symbol tensorflow::functor::XXXFunctor<Eigen::GpuDevice>::Reduce<..., std::array*, >(...)" then this is
#   the problem.
TENSORFLOW_COMMON_CUDA_FLAGS:=-DGOOGLE_CUDA=1 -I$(CUDA_BASE_DIR)/include -DEIGEN_AVOID_STL_ARRAY=1

# Here are the flags we need to pass to NVCC to compile TensorFlow's CUDA
# files.
#
# - std=c++11 to enable C++11 support in NVCC, which is used extensively in
#   Tensorflow and Eigen
# - disable-warnings turns off warning messages, which are somewhat
#   spurious.
# - arch=compute_30 targets the compute model 3.0 with pre-optimized
#   binaries (ie, the grid K520 which is on the Amazon GPU instances).  Other
#   Nvidia compute models will work, but they will require extra startup
#   time as the code is pre-optimized for them.
# - Xcompiler -fPIC,-g,-O3 to control the flags passed to the host compiler
#   to make things properly integrate.
TENSORFLOW_NVCC_CUDA_FLAGS:=$(TENSORFLOW_COMMON_CUDA_FLAGS) -std=c++11 --disable-warnings -arch=compute_30 -code=sm_30 -g -O3 -Xcompiler -fPIC 

# When compiling the cuda kernels we need to use the include flags and
# some of our own
$(eval $(call set_compile_option,$(TENSORFLOW_CUDA_NVCC_BUILD),$(TENSORFLOW_BASE_INCLUDE_FLAGS) $(TENSORFLOW_CUDA_INCLUDE_FLAGS) $(TENSORFLOW_NVCC_CUDA_FLAGS)))

# Libraries we need to link with
TENSORFLOW_CUDA_LINK:=cudart cublas curand cufft

# Library path for CUDA
CUDA_LIB_PATH:=$(CUDA_BASE_DIR)/lib64

# Flags that need to be passed when linking in something that includes
# CUDA functionality.
TENSORFLOW_CUDA_LINKER_FLAGS:=-L$(CUDA_LIB_PATH) -Wl$(comma)--rpath$(comma)$(CUDA_LIB_PATH)

endif

# Here are the flags that anything that includes TensorFlow needs to
# define.  First the flags to turn off warnings that the compiler
# will output on its sourcecode
ifeq ($(toolchain),gcc)
TENSORFLOW_WARNING_FLAGS := -Wno-reorder -Wno-return-type -Wno-overflow -Wno-overloaded-virtual -Wno-parentheses -Wno-maybe-uninitialized -Wno-array-bounds -Wno-unused-function -Wno-unused-variable
endif
ifeq ($(toolchain),gcc5)
TENSORFLOW_WARNING_FLAGS := -Wno-reorder -Wno-return-type -Wno-overflow -Wno-overloaded-virtual -Wno-parentheses -Wno-maybe-uninitialized -Wno-array-bounds -Wno-unused-function -Wno-unused-variable
endif
ifeq ($(toolchain),clang)
TENSORFLOW_WARNING_FLAGS := -Wno-unused-const-variable -Wno-unused-private-field -Wno-tautological-undefined-compare -Wno-missing-braces -Wno-absolute-value -Wno-unused-function -Wno-inconsistent-missing-override -Wno-constant-conversion
endif

# Second, the include directories required
TENSORFLOW_INCLUDE_FLAGS := $(TENSORFLOW_BASE_INCLUDE_FLAGS) $(TENSORFLOW_CUDA_INCLUDE_FLAGS)


# Here are the flags required to be passed to the compiler to compile
# tensorflow files
TENSORFLOW_COMPILE_FLAGS := $(TENSORFLOW_WARNING_FLAGS) $(TENSORFLOW_INCLUDE_FLAGS) $(TENSORFLOW_COMMON_CUDA_FLAGS)

# Here is the list of files we need to compile for tensorflow to be incorporated
$(TENSORFLOW_CC_FILES):	$(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.h) | $(TENSORFLOW_INCLUDES) $(INC)/external/re2 $(INC)/external/jpeg_archive/jpeg-9a $(INC)/external/eigen_archive/eigen-eigen-$(TENSORFLOW_EIGEN_MERCURIAL_HASH) $(HOSTBIN)/protoc $(LIB)/libprotobuf3.so $(INC)/google/protobuf

# Turn the list of files we just collected into relative pathnames that our
# rule system can understand to turn it into a library.
TENSORFLOW_CC_BUILD:=$(sort $(TENSORFLOW_CC_FILES:$(CWD)/%=%))

# We need to set up include paths and turn off a bunch of warnings.  Most of
# them are triggered by the (3rd party) Eigen library.
$(eval $(call set_compile_option,$(TENSORFLOW_CC_BUILD) $(TENSORFLOW_PROTOBUF_BUILD),$(TENSORFLOW_COMPILE_FLAGS)))

# Libraries that the core of TensorFlow relies on.  We need the CUDA
# libraries, if support is included, since the core includes the
# functionality to set up the CUDA support.
TENSORFLOW_CORE_LINK := protobuf3 re2 png jpeg $(TENSORFLOW_CUDA_LINK)

# Finally, build a library with the tensorflow functionality inside
$(eval $(call library,tensorflow-core,$(TENSORFLOW_CC_BUILD) $(TENSORFLOW_PROTOBUF_BUILD),$(TENSORFLOW_CORE_LINK),,,,,$(TENSORFLOW_CUDA_LINKER_FLAGS)))


# Part of the C++ interface to TensorFlow is created using an automatic
# generator.  First we build the actual program, which links to the basic
# set of kernels

TENSORFLOW_CC_OP_GEN_FILES := \
	tensorflow/cc/ops/cc_op_gen.cc \
	tensorflow/cc/ops/cc_op_gen_main.cc

$(TENSORFLOW_CC_OP_GEN_FILES:%=$(CWD)/%):	$(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.h) | $(TENSORFLOW_INCLUDES) $(INC)/external/re2 $(INC)/external/jpeg_archive/jpeg-9a $(INC)/external/eigen_archive/eigen-eigen-$(TENSORFLOW_EIGEN_MERCURIAL_HASH) $(HOSTBIN)/protoc $(LIB)/libprotobuf3.so  $(INC)/google/protobuf

$(eval $(call set_compile_option,$(TENSORFLOW_CC_OP_GEN_FILES),$(TENSORFLOW_COMPILE_FLAGS)))

$(eval $(call program,cc_op_gen,tensorflow-core,$(TENSORFLOW_CC_OP_GEN_FILES)))


# Now we have that program, we link it to one set of operations at a time and
# generate the files.  First, though, we need to compile each of those as a
# separate module

TENSORFLOW_OPS_SOURCES := $(shell find $(CWD)/tensorflow/core/ops -name "*_ops.cc")
TENSORFLOW_OPS := $(TENSORFLOW_OPS_SOURCES:$(CWD)/tensorflow/core/ops/%_ops.cc=%)

# Each op file may include protobuf files or other headers, so these are also
# dependent on those.
$(TENSORFLOW_OPS_SOURCES): $(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.h) | $(TENSORFLOW_INCLUDES) $(INC)/external/re2 $(INC)/external/jpeg_archive/jpeg-9a $(HOSTBIN)/protoc

# Create a library for each of the ops.  We also have to adjust the compilation
# flags so that it actually compiles
$(foreach op,$(TENSORFLOW_OPS), \
	$(eval $(call set_compile_option,tensorflow/core/ops/$(op)_ops.cc,$(TENSORFLOW_COMPILE_FLAGS))) \
	$(eval $(call library,tensorflow_$(op)_ops,tensorflow/core/ops/$(op)_ops.cc)) \
)

# Put them all into one convenient library, along with the no_op.  The no_op
# has to be here, and not in the core library, as otherwise the interface is
# generated for each of the ops and we have multiple definitions.
$(CWD)/tensorflow/core/ops/no_op.cc: | $(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.h)
$(eval $(call set_compile_option,tensorflow/core/ops/no_op.cc,$(TENSORFLOW_COMPILE_FLAGS)))
$(eval $(call library,tensorflow-ops,tensorflow/core/ops/no_op.cc,$(foreach op,$(TENSORFLOW_OPS),tensorflow_$(op)_ops) tensorflow-core,,,,,$(TENSORFLOW_CUDA_LINKER_FLAGS)))


# Now the kernels
TENSORFLOW_KERNEL_CC_FILES:=$(shell find $(CWD)/tensorflow/core/kernels -name "*.cc" | grep -v test | grep -v '\.cu\.cc' | grep -v '\.pb\.cc' | grep -v 'fact_op\.cc')

TENSORFLOW_KERNEL_CC_BUILD:=$(sort $(TENSORFLOW_KERNEL_CC_FILES:$(CWD)/%=%))

$(eval $(call set_compile_option,$(TENSORFLOW_KERNEL_CC_BUILD) $(TENSORFLOW_PROTOBUF_BUILD),$(TENSORFLOW_COMPILE_FLAGS) -mavx))

$(TENSORFLOW_KERNEL_CC_FILES):	$(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.h) | $(TENSORFLOW_INCLUDES) $(INC)/external/re2 $(INC)/external/jpeg_archive/jpeg-9a $(INC)/external/eigen_archive/eigen-eigen-$(TENSORFLOW_EIGEN_MERCURIAL_HASH) $(HOSTBIN)/protoc  $(INC)/google/protobuf
$(eval $(call library,tensorflow-kernels,$(TENSORFLOW_KERNEL_CC_BUILD) $(TENSORFLOW_CUDA_NVCC_BUILD),tensorflow-ops $(TENSORFLOW_CUDA_LINK),,,,,$(TENSORFLOW_CUDA_LINKER_FLAGS)))



# Overall library that wraps it all together
$(eval $(call library,tensorflow,,tensorflow-ops tensorflow-kernels tensorflow-core))

# And create a nice target name
tensorflow_lib: $(LIB)/libtensorflow.so

# Rule to auto-generate source code for the c interface.
#
# We preload the operations library and then run the tool to generate the code.
#
# The user_ops.h header is used as a placeholder for those who want to extend
# tensorflow in-tree.  Since MLDB provides other means for extension, and we
# don't want to modify the source tree, we put a dummy, empty header in there
# to signify that there are no user operations available.
$(CWD)/tensorflow/cc/ops/%_ops.cc $(CWD)/tensorflow/cc/ops/%_ops.h:	$(BIN)/cc_op_gen $(LIB)/libtensorflow_%_ops.so
	@LD_PRELOAD=$(LIB)/libtensorflow_$(*)_ops.so $(BIN)/cc_op_gen $(TF_CWD)/tensorflow/cc/ops/$(*)_ops.h $(TF_CWD)/tensorflow/cc/ops/$(*)_ops.cc 0
	@touch $(TF_CWD)/tensorflow/cc/ops/user_ops.h

# Find the source files needed for the C++ interface.  Some are pre-packaged and
# others were generated above.
TENSORFLOW_CC_INTERFACE_FILES:=$(sort $(shell (find $(CWD)/tensorflow/cc -name "*.cc" | grep -v example)) $(foreach op,$(TENSORFLOW_OPS),$(CWD)/tensorflow/cc/ops/$(op)_ops.cc))

# Each of these may include protobuf files or other headers, so these are also
# dependent on those.
$(TENSORFLOW_CC_INTERFACE_FILES): $(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.h) | $(TENSORFLOW_INCLUDES) $(INC)/external/re2 $(INC)/external/jpeg_archive/jpeg-9a $(HOSTBIN)/protoc  $(INC)/google/protobuf

# Turn them into arguments for the library macro by stripping the CWD
TENSORFLOW_CC_INTERFACE_BUILD:=$(TENSORFLOW_CC_INTERFACE_FILES:$(CWD)/%=%)

# Create the interface library
$(eval $(call set_compile_option,$(TENSORFLOW_CC_INTERFACE_BUILD),$(TENSORFLOW_COMPILE_FLAGS)))
$(eval $(call library,tensorflow-cpp-interface,$(TENSORFLOW_CC_INTERFACE_BUILD),tensorflow))

# This variable can be used by something that includes tensorflow to say that
# it depends on the tensorflow include files.
DEPENDS_ON_TENSORFLOW_HEADERS:=$(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.h) $(foreach op,$(TENSORFLOW_OPS),$(CWD)/tensorflow/cc/ops/$(op)_ops.h)

endif
