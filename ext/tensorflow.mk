# tensorflow.mk
# Jeremy Barnes, 17 December 2015
# Copyright (c) 2015 mldb.ai inc.
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

# Should only need this while we're upgrading... Jenkins gets confused
hack:=$(shell rm -f ./mldb/ext/tensorflow/tensorflow/cc/ops/attention_ops.cc ./mldb/ext/tensorflow/tensorflow/cc/ops/summary_ops.cc)

comma=,

# Copy the current working directory, which can't be used inside a rule since it's
# expanded once the rule runs, not when it's instantiated
TF_CWD:=$(CWD)

# Tensorflow comes with lists of files that it needs compiled in its
# makefile
TF_MAKEFILE_DIR:=mldb/ext/tensorflow/tensorflow/contrib/makefile

# What sources we want to compile, derived from the main Bazel build using the
# gen_file_lists.sh script.
PBT_CC_SRCS := $(shell cat $(TF_MAKEFILE_DIR)/tf_pb_text_files.txt)
PROTO_SRCS := $(shell cat $(TF_MAKEFILE_DIR)/tf_proto_files.txt)

# Flags required to include the eigen linear algebra framework.  This
# has moved around a fair bit over the Tensorflow releases.
TENSORFLOW_EIGEN_INCLUDES:=-Imldb/ext/eigen -I$(CWD)/third_party/eigen3

# Include flags for all of Tensorflow
TENSORFLOW_BASE_INCLUDE_FLAGS := -I$(CWD) -Imldb/ext/re2 -I$(BUILD)/$(ARCH)/tmp/tensorflow-includes $(TENSORFLOW_EIGEN_INCLUDES) -Imldb/ext -DPLATFORM_POSIX=1 -I$(PROTOBUF_INCLUDE_DIR) -Imldb/ext/highwayhash

# There is a lightweight Protobuf format that is created by this program
# which is distributed with Tensorflow

# The list of dependencies is derived from the Bazel build file by running
# the gen_file_lists.sh script on a system with a working Bazel setup.
PROTO_TEXT_CC_FILES := $(shell cat $(TF_MAKEFILE_DIR)/proto_text_cc_files.txt)

TENSORFLOW_PROTO_TEXT_FILES := $(PROTO_TEXT_CC_FILES)

$(TENSORFLOW_PROTO_TEXT_FILES:%=$(CWD)/%): | $(TENSORFLOW_INCLUDES) $(DEPENDS_ON_PROTOBUF_INCLUDES) $(CWD)/tensorflow/core/lib/core/error_codes.pb.h $(CWD)/tensorflow/core/framework/summary.pb.h

$(eval $(call set_compile_option,$(TENSORFLOW_PROTO_TEXT_FILES),$(TENSORFLOW_BASE_INCLUDE_FLAGS) -Wno-unused-private-field -Wno-unused-const-variable))

$(eval $(call program,proto_text,protobuf3 z,$(TENSORFLOW_PROTO_TEXT_FILES)))

# Compile all of the .proto files in the TensorFlow
# source directory.  Here we find all of those files
TENSORFLOW_PROTOBUF_FILES:=$(shell find $(CWD)/tensorflow/core -name "*.proto" | grep -v 'protobuf/worker')
TENSORFLOW_PROTOBUF_TEXT_FILES:=$(filter-out %/test_log.proto %/saved_model.proto %/meta_graph.proto,$(TENSORFLOW_PROTOBUF_FILES))
TENSORFLOW_PROTOBUF_BUILD:=$(TENSORFLOW_PROTOBUF_FILES:$(CWD)/%.proto=%.pb.cc) $(TENSORFLOW_PROTOBUF_TEXT_FILES:$(CWD)/%.proto=%.pb_text.cc)
TENSORFLOW_PROTOBUF_HEADERS:=$(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.h) $(TENSORFLOW_PROTOBUF_TEXT_FILES:%.proto=%.pb_text.h)

# Find all of the .cc files that we need to bundle up to run the tensorflow
# kernels.  We strip out any Cuda constructs, and add them back in later in
# the part of the makefile designed around Cuda support.

TENSORFLOW_CC_FILES:=$(shell (find $(CWD)/tensorflow/core -name "*.cc"; find $(CWD)/tensorflow/stream_executor -name "*.cc") | grep -v '\.cu\.cc' | grep -v '\.pb\.cc' | grep -v '.*_ops\.cc' | grep -v 'ops/no_op\.cc' | grep -v test | grep -v tutorial | grep -v user_ops | grep -v 'fact_op\.cc' $(if $(WITH_CUDA),, | grep -v cuda  $(if $(WITH_CUDNN),,| grep -v cu_dnn) ) | grep -v /kernels/ | grep -v distributed_runtime | grep -v platform/cloud | grep -v pb_text $(if $(WITH_CUDA),,| grep -v 'gpu/.*\.cc' | grep -v 'stream_executor/machine_manager.cc\|stream_executor/dso_loader.cc')) $(CWD)/tensorflow/core/common_runtime/gpu/gpu_tracer.cc

# Tensorflow comes bundled with scripts that download various external things
# that we already have as a system dependency, and hardcodes the include
# path.  We create include directories that forward to /usr/include to point
# to the system versions, or include them as submodules and create a fake
# include directory for tensorflow to find them under.
TENSORFLOW_FAKE_INCLUDE_DIR:=$(BUILD)/$(ARCH)/tmp/tensorflow-includes

# What version of farmhash do we need?
TENSORFLOW_FARMHASH_HASH:=$(shell grep 'prefix_dir' mldb/ext/tensorflow/farmhash.BUILD | head -n1 | sed 's/.*"farmhash-\(.*\)".*/\1/')
$(if $(TENSORFLOW_FARMHASH_HASH),,$(error Couldnt find farmhash hash))

$(TENSORFLOW_FAKE_INCLUDE_DIR)/farmhash-$(TENSORFLOW_FARMHASH_HASH):
	@mkdir -p $(dir $(@)) && ln -sf $(PWD)/mldb/ext/farmhash $(@)

# Finally, we need eigen3.  It includes it with the specific Mercurial
# hash needed, which we list here.  This is also included in ext.

# What version of eigen do we need?
TENSORFLOW_EIGEN_MERCURIAL_HASH:=$(shell grep 'eigen_version' mldb/ext/tensorflow/tensorflow/workspace.bzl | head -n1 | sed 's/.*eigen_version = "\(.*\)".*/\1/')
$(if $(TENSORFLOW_EIGEN_MERCURIAL_HASH),,$(error Couldnt find Eigen hash.  You may need to run `git submodule update --init --recursive` to get all the required submodules.))

$(TENSORFLOW_FAKE_INCLUDE_DIR)/eigen_archive/eigen-eigen-$(TENSORFLOW_EIGEN_MERCURIAL_HASH):
	@mkdir -p $(dir $(@)) && ln -sf $(PWD)/mldb/ext/eigen $(@)

# We take the system libpng, and just put it where tensorflow can include it
$(TENSORFLOW_FAKE_INCLUDE_DIR)/libpng-1.2.53:
	@mkdir -p $(dir $(@)) && ln -s /usr/include $(@)



# This is a list of all of them, which acts as a dependency target
TENSORFLOW_INCLUDES:= \
	$(TENSORFLOW_FAKE_INCLUDE_DIR)/libpng-1.2.53 \
	$(TENSORFLOW_FAKE_INCLUDE_DIR)/eigen_archive/eigen-eigen-$(TENSORFLOW_EIGEN_MERCURIAL_HASH) \
	$(TENSORFLOW_FAKE_INCLUDE_DIR)/farmhash-$(TENSORFLOW_FARMHASH_HASH) \


# To create a protobuf file, we compile the input file.  Same for the .h
# file.
$(CWD)/%.pb.cc $(CWD)/%.pb.h:		$(CWD)/%.proto | $(DEPENDS_ON_PROTOC) $(DEPENDS_ON_PROTOBUF_INCLUDES)
	@echo "         $(COLOR_CYAN)[PROTO]$(COLOR_RESET)			$(basename $<)"
	@$(PROTOC) $< -Imldb/ext/tensorflow --cpp_out=$(TF_CWD)

# Same for pb_text files
$(CWD)/%.pb_text.cc $(CWD)/%.pb_text.h $(CWD)/%.pb_text-impl.h:	$(CWD)/%.proto | $(HOSTBIN)/proto_text $(DEPENDS_ON_PROTOBUF_INCLUDES)
	@echo "      $(COLOR_CYAN)[PROTOTXT]$(COLOR_RESET)			$(basename $<)"
	@$(HOSTBIN)/proto_text mldb/ext/tensorflow/tensorflow/core tensorflow/core mldb/ext/tensorflow/tensorflow/tools/proto_text/placeholder.txt $<


# CUDA base defines, used even with no CUDA support
CUDA_VERSION?=8.0
CUDNN_VERSION?=5

# Which CUDA capabilities do we want to pre-build for?
# https://developer.nvidia.com/cuda-gpus
#   Compute/shader model   Cards
#   6.1			   P4, P40, Titan X
#   6.0                    P100
#   5.2                    M40
#   3.7                    K80
#   3.5                    K40, K20
#   3.0                    K10, Grid K520 (AWS G2)
#   Other Nvidia shader models should work, but they will require extra startup
#   time as the code is pre-optimized for them.
CUDA_MODELS=30 35 37 52 60 61

# Which CUDA compute capabilities do we support?  3.0 is needed for the
# AWS g2 instances; this is the lowest common denominator
TF_CUDA_CAPABILITIES?=3.0

# This part deals with CUDA support.  It's only enabled if WITH_CUDA is
# equal to 1.
ifeq ($(WITH_CUDA),1)

# Which version of CUDA do we use?
CUDA_VERSION?=8.0

# This is the directory that the whole CUDA development kit is installed
# inside.
CUDA_BASE_DIR?=/usr/local/cuda-$(CUDA_VERSION)

# This is the directory that the CUDA header files are included in.
# If it's not passed into the makefile
CUDA_SYSTEM_HEADER_DIR?=$(CUDA_BASE_DIR)/include

# The nvcc compiler, used to compile .cu.cc files.
NVCC?=$(CUDA_BASE_DIR)/bin/nvcc

# These are the CUDA header files that are used from within Tensorflow
# and expected to live under the third_party/gpus/cuda/include directory.
# We link them individually in the directory that they're expected in
# to avoid issues with header name clashes.

CUDA_HEADERS := cuda.h cublas.h cufft.h cuComplex.h vector_types.h builtin_types.h device_types.h host_defines.h driver_types.h surface_types.h texture_types.h cublas_v2.h curand.h cuda_runtime.h cublas_api.h cuda_fp16.h $(if $(WITH_CUDNN),cudnn.h)

# If we are using CUDA, we also need to link its include directory into
# third_party/gpus/cuda/include

$(INC)/third_party/gpus/cuda/include/%: | $(CUDA_SYSTEM_HEADER_DIR)/%
	@mkdir -p $(dir $@)
	@ln -sf $(CUDA_SYSTEM_HEADER_DIR)/$(notdir $@) $@

$(INC)/third_party/gpus/cuda/extras:	| $(CUDA_BASE_DIR)/extras
	@mkdir -p $(dir $@)
	@ln -sf $(CUDA_BASE_DIR)/extras $@

$(INC)/third_party/gpus/cuda/include/cudnn.h: /usr/include/x86_64-linux-gnu/cudnn_v5.h
	@mkdir -p $(dir $@)
	@ln -s $< $@

# Anything that's to do with Cuda depends on these header files, so set
# up the dependency.

$(TENSORFLOW_CC_FILES): \
	$(foreach header,$(CUDA_HEADERS),$(INC)/third_party/gpus/cuda/include/$(header)) | $(INC)/third_party/gpus/cuda/extras $(INC)/third_party/gpus/cuda/include/cudnn.h $(INC)/third_party/gpus/cuda/cuda_config.h

# Some of the CUDA headers include their header dependencies via <file.h>
# instead of "file.h", which requires us to set up the system header directory
# to find them in.
TENSORFLOW_CUDA_INCLUDE_FLAGS:=-Isystem$(CUDA_SYSTEM_HEADER_DIR) -I$(CUDA_SYSTEM_HEADER_DIR) -I$(INC)/third_party/gpus/cuda/extras/CUPTI/include

# Now we find all of the cuda files that need to be compiled by nvidia's
# nvcc compiler.  These are anything that end in .cu.cc
TENSORFLOW_CUDA_NVCC_FILES:=$(shell (find $(CWD)/tensorflow -name "*.cu.cc") | grep -v how_tos | grep -v contrib/)

# They depend on protobuf
$(TENSORFLOW_CUDA_NVCC_FILES): | $(DEPENDS_ON_PROTOBUF_INCLUDES)

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
# - EIGEN_HAS_VARIADIC_TEMPLATES is required for cross-compiling, where
#   Eigen disables needed std::initializer_list constructors in its arrays
#   because it doesn't properly detect that this feature is available
# - -DEigenCudaErrorFunctionDeclaration,-DcheckForCudaSuccess are to
#   install a custom error handler that doesn't crash MLDB on a transient
#   CUDA failure
TENSORFLOW_COMMON_CUDA_FLAGS=\
	-DGOOGLE_CUDA=1 \
	-I$(CUDA_BASE_DIR)/include \
	-DTF_EXTRA_CUDA_CAPABILITIES=$(TF_CUDA_CAPABILITIES) \
	-DEIGEN_AVOID_STL_ARRAY=1 \
	-DEIGEN_HAS_VARIADIC_TEMPLATES=1 \
	-DEigenCudaErrorFunctionDeclaration="void mldbCheckForCudaSuccess(cudaError_t);" \
	-DcheckForCudaSuccess="mldbCheckForCudaSuccess" \
	-I$(INC)/third_party/gpus 

# Here are the flags we need to pass to NVCC to compile TensorFlow's CUDA
# files.
#
# - std=c++11 to enable C++11 support in NVCC, which is used extensively in
#   Tensorflow and Eigen
# - disable-warnings turns off warning messages, which are somewhat
#   spurious.
# - -gencode=... targets the following compute models with pre-optimized
#   binaries:
# - Xcompiler -fPIC,-g,-O3 to control the flags passed to the host compiler
#   to make things properly integrate.
# - --compiler-bindir: set up the proper backend compiler for our
#   architecture to compile the CPU portions of the CUDA kernels
# - EIGEN_HAS_VARIADIC_TEMPLATES is required for cross-compiling, where
#   Eigen disables needed std::initializer_list constructors in its arrays
#   because it doesn't properly detect that this feature is available
# - For cross-compilation, we tell it exactly what the underlying compiler
#   is so that it gets it right
#   See here: http://docs.nvidia.com/cuda/cuda-installation-guide-linux/index.html#cross-platform

TENSORFLOW_NVCC_CUDA_FLAGS=$(TENSORFLOW_COMMON_CUDA_FLAGS) -std=c++11 --disable-warnings $(foreach model,$(CUDA_MODELS),-gencode arch=compute_$(model)$(comma)code=sm_$(model)) -g -O3 -Xcompiler -fPIC --compiler-bindir=$(lastword $(CXX)) $(TENSORFLOW_ARCH_CUDA_FLAGS) -DEIGEN_HAS_VARIARDIC_TEMPLATES=1

# When compiling the cuda kernels we need to use the include flags and
# some of our own
$(eval $(call set_compile_option,$(TENSORFLOW_CUDA_NVCC_BUILD),$$(TENSORFLOW_BASE_INCLUDE_FLAGS) $$(TENSORFLOW_CUDA_INCLUDE_FLAGS) $$(TENSORFLOW_NVCC_CUDA_FLAGS)))

# Libraries we need to link with.
TENSORFLOW_CUDA_LINK:=  cudart cublas curand cufft

# Library path for CUDA
CUDA_LIB_PATH?=$(CUDA_BASE_DIR)/lib64

# Flags that need to be passed when linking in something that includes
# CUDA functionality.
TENSORFLOW_CUDA_LINKER_FLAGS:=-L$(CUDA_LIB_PATH) -Wl$(comma)--rpath$(comma)$(CUDA_LIB_PATH)

endif

# This header is used even when CUDA isn't compiled in, ie WITH_CUDA:=1
$(INC)/third_party/gpus/cuda/cuda_config.h:
	@mkdir -p $(dir $@)
	@echo "//Auto-generated by MLDB tensorflow.mk, do not edit" > $@~
	@echo "#define TF_CUDA_VERSION \"$(CUDA_VERSION)\"" >> $@~
	@echo "#define TF_CUDNN_VERSION \"$(CUDNN_VERSION)\"" >> $@~
	@echo "#define TF_CUDA_CAPABILITIES CudaVersion(\"$(TF_CUDA_CAPABILITIES)\")" >> $@~
	@mv $@~ $@


# Here are the flags that anything that includes TensorFlow needs to
# define.  First the flags to turn off warnings that the compiler
# will output on its sourcecode
ifeq ($(toolchain),gcc)
TENSORFLOW_WARNING_FLAGS := -Wno-reorder -Wno-return-type -Wno-overflow -Wno-overloaded-virtual -Wno-parentheses -Wno-maybe-uninitialized -Wno-array-bounds -Wno-unused-function -Wno-unused-variable -Wno-uninitialized -Wno-comment -Wno-narrowing -Wno-strict-aliasing
endif
ifeq ($(toolchain),gcc5)
TENSORFLOW_WARNING_FLAGS := -Wno-reorder -Wno-return-type -Wno-overflow -Wno-overloaded-virtual -Wno-parentheses -Wno-maybe-uninitialized -Wno-array-bounds -Wno-unused-function -Wno-unused-variable -Wno-uninitialized -Wno-comment -Wno-narrowing -Wno-strict-aliasing
endif
ifeq ($(toolchain),gcc6)
TENSORFLOW_WARNING_FLAGS := -Wno-reorder -Wno-return-type -Wno-overflow -Wno-overloaded-virtual -Wno-parentheses -Wno-maybe-uninitialized -Wno-array-bounds -Wno-unused-function -Wno-unused-variable -Wno-uninitialized -Wno-comment -Wno-ignored-attributes -Wno-nonnull-compare -Wno-narrowing -Wno-strict-aliasing
endif
ifeq ($(toolchain),clang)
# -Wno-error should not be used but so far it seems like the only way to mute
# warning: braces around scalar initializer
TENSORFLOW_WARNING_FLAGS := -Wno-unused-const-variable -Wno-unused-private-field -Wno-tautological-undefined-compare -Wno-missing-braces -Wno-absolute-value -Wno-unused-function -Wno-inconsistent-missing-override -Wno-constant-conversion -Wno-unused-variable -Wno-error -Wno-braced-scalar-init
endif

# Second, the include directories required
TENSORFLOW_INCLUDE_FLAGS := $(TENSORFLOW_BASE_INCLUDE_FLAGS) $(TENSORFLOW_CUDA_INCLUDE_FLAGS)


# Here are the flags required to be passed to the compiler to compile
# tensorflow files
TENSORFLOW_COMPILE_FLAGS := $(TENSORFLOW_WARNING_FLAGS) $(TENSORFLOW_INCLUDE_FLAGS) $(TENSORFLOW_COMMON_CUDA_FLAGS) -DEIGEN_AVOID_STL_ARRAY=1

# Here is the list of files we need to compile for tensorflow to be incorporated
$(TENSORFLOW_CC_FILES):	| $(TENSORFLOW_INCLUDES) $(DEPENDS_ON_PROTOBUF_INCLUDES)

# Those which are not required to build the proto-text compiler also
# depend on protobuf files
$(filter-out $(addprefix $(CWD)/,$(PROTO_TEXT_CC_FILES)),$(TENSORFLOW_CC_FILES)): $(TENSORFLOW_PROTOBUF_HEADERS)

# Turn the list of files we just collected into relative pathnames that our
# rule system can understand to turn it into a library.
TENSORFLOW_CC_BUILD:=$(sort $(TENSORFLOW_CC_FILES:$(CWD)/%=%))

# We need to set up include paths and turn off a bunch of warnings.  Most of
# them are triggered by the (3rd party) Eigen library.
$(eval $(call set_compile_option,$(TENSORFLOW_CC_BUILD) $(TENSORFLOW_PROTOBUF_BUILD) ../tf_cuda_handlers.cc,$$(TENSORFLOW_COMPILE_FLAGS)))

# Libraries that the core of TensorFlow relies on.  We need the CUDA
# libraries, if support is included, since the core includes the
# functionality to set up the CUDA support.
TENSORFLOW_CORE_LINK := protobuf3 re2 png jpeg farmhash giflib $(TENSORFLOW_CUDA_LINK)

# Finally, build a library with the tensorflow functionality inside
$(eval $(call library,tensorflow-core,$(TENSORFLOW_CC_BUILD) $(TENSORFLOW_PROTOBUF_BUILD) $(if $(WITH_CUDA),../tf_cuda_handlers.cc),$(TENSORFLOW_CORE_LINK),,,,,$(TENSORFLOW_CUDA_LINKER_FLAGS)))


# Part of the C++ interface to TensorFlow is created using an automatic
# generator.  First we build the actual program, which links to the basic
# set of kernels

TENSORFLOW_CC_OP_GEN_FILES := \
	tensorflow/cc/framework/cc_op_gen.cc \
	tensorflow/cc/framework/cc_op_gen_main.cc

$(TENSORFLOW_CC_OP_GEN_FILES:%=$(CWD)/%):	$(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.h) | $(TENSORFLOW_INCLUDES) $(DEPENDS_ON_PROTOBUF_INCLUDES) 

$(eval $(call set_compile_option,$(TENSORFLOW_CC_OP_GEN_FILES),$$(TENSORFLOW_COMPILE_FLAGS)))

$(eval $(call program,cc_op_gen,tensorflow-core,$(TENSORFLOW_CC_OP_GEN_FILES)))


# Now we have that program, we link it to one set of operations at a time and
# generate the files.  First, though, we need to compile each of those as a
# separate module

# Stop old files from breaking the build on an upgrade.  Should only need
# until we're done with the upgrade.
TENSORFLOW_OPS_SOURCES := $(shell find $(CWD)/tensorflow/core/ops -name "*_ops.cc")
#$(warning TENSORFLOW_OPS_SOURCES=$(TENSORFLOW_OPS_SOURCES))
TENSORFLOW_OPS := $(filter-out attention summary,$(TENSORFLOW_OPS_SOURCES:$(CWD)/tensorflow/core/ops/%_ops.cc=%))
#$(warning TENSORFLOW_OPS=$(TENSORFLOW_OPS))

# Each op file may include protobuf files or other headers, so these are also
# dependent on those.
$(TENSORFLOW_OPS_SOURCES): $(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.h) | $(TENSORFLOW_INCLUDES)

# Create a library for each of the ops.  We also have to adjust the compilation
# flags so that it actually compiles
$(foreach op,$(TENSORFLOW_OPS), \
	$(eval $(call set_compile_option,tensorflow/core/ops/$(op)_ops.cc,$$(TENSORFLOW_COMPILE_FLAGS))) \
	$(eval $(call library,tensorflow_$(op)_ops,tensorflow/core/ops/$(op)_ops.cc)) \
)

# Put them all into one convenient library, along with the no_op.  The no_op
# has to be here, and not in the core library, as otherwise the interface is
# generated for each of the ops and we have multiple definitions.
$(CWD)/tensorflow/core/ops/no_op.cc: | $(DEPENDS_ON_PROTOBUF_INCLUDES) $(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.h)
$(eval $(call set_compile_option,tensorflow/core/ops/no_op.cc,$$(TENSORFLOW_COMPILE_FLAGS)))
$(eval $(call library,tensorflow_no_op,tensorflow/core/ops/no_op.cc))

$(eval $(call library,tensorflow-ops,,$(foreach op,$(TENSORFLOW_OPS),tensorflow_$(op)_ops) tensorflow_no_op tensorflow-core,,,,,$(TENSORFLOW_CUDA_LINKER_FLAGS)))


# Now the kernels
TENSORFLOW_KERNEL_CC_FILES:=$(shell find $(CWD)/tensorflow/core/kernels -name "*.cc" | grep -v test | grep -v '\.cu\.cc' | grep -v '\.pb\.cc' | grep -v 'fact_op\.cc' | grep -v '\#' | grep -v 'debug_ops')

TENSORFLOW_KERNEL_CC_BUILD:=$(sort $(TENSORFLOW_KERNEL_CC_FILES:$(CWD)/%=%))

# Dependencies for the kernel files... basically everything else needs to
# be built first
$(TENSORFLOW_KERNEL_CC_FILES):	$(TENSORFLOW_PROTOBUF_HEADERS) | $(TENSORFLOW_INCLUDES) $(DEPENDS_ON_PROTOBUF_INCLUDES) $(INC)/third_party/gpus/cuda/cuda_config.h

# This is a list of variants for each architecture
# Note that this can cause long compile times as a big chunk of
# TensorFlow is built multiple times, in that case you
# might want to set TF_KERNEL_VARIANTS:=(whichever one applies) to
# speed up compile times
TF_KERNEL_VARIANTS_x86_64?=sse42 avx avx2
TF_KERNEL_VARIANTS_aarch64:=generic
TF_KERNEL_VARIANTS_arm:=generic

# For each variant, this provides the optimization flags passed in
# The correct kernels library is detected at runtime and loaded
# dynamically on Tensorflow initialization
TF_KERNEL_VARIANT_x86_64_sse42:=-msse4.2
TF_KERNEL_VARIANT_x86_64_avx:=-mavx
TF_KERNEL_VARIANT_x86_64_avx2:=-mavx2 -mfma

# ARM variants don't currently detect their CPU capabilities at
# runtime, so only a generic variant is compiled
TF_KERNEL_VARIANT_aarch64_generic:=-funsafe-math-optimizations -ftree-vectorize -Os
TF_KERNEL_VARIANT_arm_generic:=-funsafe-math-optimizations -ftree-vectorize -Os

# By default, we compile all kernel variants for our architecture
# This can be overridden to speed up compile times
TF_KERNEL_VARIANTS?=$(TF_KERNEL_VARIANTS_$(ARCH))

# If no kernel variants are specified, we can't build kernels
# and so we will fail at compile time
$(if $(TF_KERNEL_VARIANTS),,$(error TF_KERNEL_VARIANTS_$(ARCH) is not set; when cross-compiling you need to set up architecture variants))

# Macro to define a kernel variant for compilation
# This will create a library tensorflow-kernels-(variant) that can be
# loaded at runtime depending upon the detected architecture.
#
# To create this, we
# a) copy the given files to a temporary directory so that they are seen
#    as unique by the build system and can thus have different compile
#    flags
# b) compile them with the custom flags
#
# $(1) = variant name

define tf_kernel_variant

# Rule to copy the source files over to tmp 
mldb/ext/tensorflow/../../$(BUILD)/$(ARCH)/tmp/tensorflow-kernel-variants/$(1)/%:	$(CWD)/%
	@mkdir -p $$(dir $$@) && cp $$< $$@~ && mv $$@~ $$@

TENSORFLOW_KERNEL_BUILD_VARIANT_$(1):=$$(addprefix ../../$(BUILD)/$(ARCH)/tmp/tensorflow-kernel-variants/$(1)/,$$(TENSORFLOW_KERNEL_CC_BUILD))

$$(eval $$(call set_compile_option,$$(TENSORFLOW_KERNEL_BUILD_VARIANT_$(1)),$$(TENSORFLOW_COMPILE_FLAGS) $$(TF_KERNEL_VARIANT_$(ARCH)_$(1))))
$$(eval $$(call library,tensorflow-kernels-$(1),$$(TENSORFLOW_KERNEL_BUILD_VARIANT_$(1)) $$(TENSORFLOW_CUDA_NVCC_BUILD),tensorflow-ops $$(TENSORFLOW_CUDA_LINK),,,,,$$(TENSORFLOW_CUDA_LINKER_FLAGS),$(BUILD)/$(ARCH)/tmp/tensorflow-kernel-variants))

# Tensorflow depends upon having this variant available
$(LIB)/libtensorflow.so: $(LIB)/libtensorflow-kernels-$(1).so

#$(w arning $(LIB)/libtensorflow.so: $(LIB)/libtensorflow-kernels-$(1).so)

endef

$(foreach variant,$(TF_KERNEL_VARIANTS),$(eval $(call tf_kernel_variant,$(variant))))

# Overall library that wraps it all together
$(eval $(call library,tensorflow,../tf_load_kernels.cc,tensorflow-ops tensorflow-core))


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

define generate_tensorflow_op

$(CWD)/tensorflow/cc/ops/$(1)_ops.cc:	$(HOSTBIN)/cc_op_gen $(BUILD)/$(HOSTARCH)/lib/libtensorflow_$(1)_ops.so | $(DEPENDS_ON_PROTOBUF_INCLUDES)
	@LD_PRELOAD=$(BUILD)/$(HOSTARCH)/lib/libtensorflow_$(1)_ops.so $(HOSTBIN)/cc_op_gen $(TF_CWD)/tensorflow/cc/ops/$(1)_ops.h $(TF_CWD)/tensorflow/cc/ops/$(1)_ops.cc 0
	@touch $(TF_CWD)/tensorflow/cc/ops/user_ops.h

$(CWD)/tensorflow/cc/ops/$(1)_ops.h: | $(CWD)/tensorflow/cc/ops/$(1)_ops.cc

# We need these libraries to be available for the build to run, so make them part
# of build_tools
build_tools:	$(BUILD)/$(HOSTARCH)/lib/libtensorflow_$(1)_ops.so

endef

$(foreach op,$(TENSORFLOW_OPS),$(eval $(call generate_tensorflow_op,$(op))))

# Generate the no-op
$(CWD)/tensorflow/cc/ops/no_op.cc:	$(HOSTBIN)/cc_op_gen $(BUILD)/$(HOSTARCH)/lib/libtensorflow_no_op.so | $(DEPENDS_ON_PROTOBUF_INCLUDES)
	@LD_PRELOAD=$(BUILD)/$(HOSTARCH)/lib/libtensorflow_no_op.so $(HOSTBIN)/cc_op_gen $(TF_CWD)/tensorflow/cc/ops/no_op.h $(TF_CWD)/tensorflow/cc/ops/no_op.cc 0
	@touch $(TF_CWD)/tensorflow/cc/ops/user_op.h

$(CWD)/tensorflow/cc/ops/no_op.h: | $(CWD)/tensorflow/cc/ops/no_op.cc

build_tools: $(BUILD)/$(HOSTARCH)/lib/libtensorflow_no_op.so

# Find the source files needed for the C++ interface.  Some are pre-packaged and
# others were generated above.
TENSORFLOW_CC_INTERFACE_FILES:=$(sort $(shell (find $(CWD)/tensorflow/cc -name "*.cc" | grep -v example | grep -v '_test.cc' | grep -v math_grad)) $(foreach op,$(TENSORFLOW_OPS),$(CWD)/tensorflow/cc/ops/$(op)_ops.cc))

# Each of these may include protobuf files or other headers, so these are also
# dependent on those.
$(TENSORFLOW_CC_INTERFACE_FILES): $(TENSORFLOW_PROTOBUF_HEADERS) | $(TENSORFLOW_INCLUDES) $(DEPENDS_ON_PROTOBUF_INCLUDES)

# Turn them into arguments for the library macro by stripping the CWD
TENSORFLOW_CC_INTERFACE_BUILD:=$(TENSORFLOW_CC_INTERFACE_FILES:$(CWD)/%=%)

# Create the interface library
$(eval $(call set_compile_option,$(TENSORFLOW_CC_INTERFACE_BUILD),$$(TENSORFLOW_COMPILE_FLAGS)))
$(eval $(call library,tensorflow-cpp-interface,$(TENSORFLOW_CC_INTERFACE_BUILD),tensorflow))

# This variable can be used by something that includes tensorflow to say that
# it depends on the tensorflow include files.
DEPENDS_ON_TENSORFLOW_HEADERS:=$(TENSORFLOW_PROTOBUF_HEADERS) $(foreach op,$(TENSORFLOW_OPS),$(CWD)/tensorflow/cc/ops/$(op)_ops.h) $(CWD)/tensorflow/cc/ops/no_op.h

# Finally, we need some host build tools to make everything work.  Those are defined here.

build_tools:   $(HOSTBIN)/cc_op_gen $(HOSTBIN)/proto_text



endif
