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
	cd ext/tensorflow/google/protobuf && ./autogen.sh && ./configure --prefix $(PWD)/$(BUILD)/$(HOSTARCH) && $(MAKE) -j install

# Now we have it, we use it to compile all of the .proto files in the TensorFlow
# source directory.  Here we find all of those files
TENSORFLOW_PROTOBUF_FILES:=$(shell find $(CWD)/tensorflow/core -name "*.proto")
TENSORFLOW_PROTOBUF_BUILD:=$(TENSORFLOW_PROTOBUF_FILES:$(CWD)/%.proto=%.pb.cc)

# Find all of the .cc files that we need to bundle up to run the tensorflow
# kernels.  For the moment, we don't use any Cuda constructs, so we strip those
# out.
TENSORFLOW_CC_FILES:=$(shell (find $(CWD)/tensorflow/core -name "*.cc"; find $(CWD)/tensorflow/stream_executor -name "*.cc") | grep -v '\.cu\.cc' | grep -v '\.pb\.cc' | grep -v jpeg | grep -v test | grep -v cuda)

# Turn them into relative pathnames that our rule system can understand to turn
# it into a library.
TENSORFLOW_CC_BUILD:=$(TENSORFLOW_CC_FILES:$(CWD)/%=%)

# Tensorflow comes bundled with scripts that download various external things
# that we already have as a system dependency, and hardcodes the include
# path.  We create include directories that forward to /usr/include to point
# to the system versions.
TENSORFLOW_INCLUDES:= \
	$(INC)/external/png_archive/libpng-1.2.53 $(INC)/external/jpeg_archive/jpeg-9a

# How to actually make an include directory pointing to /usr/include
$(TENSORFLOW_INCLUDES):
	mkdir -p $(dir $(@)) && ln -s /usr/include $(@)

# It also needs re2, which we have locally in ext.  We create the necessary
# include directories.
$(INC)/external/re2:
	mkdir -p $(dir $(@)) && ln -s $(PWD)/mldb/ext/re2 $(@)

# Here is the list of files we need to compile for tensorflow to be incorporated
$(TENSORFLOW_CC_FILES):	$(TENSORFLOW_PROTOBUF_FILES:%.proto=%.pb.cc) | $(TENSORFLOW_INCLUDES) $(INC)/external/re2

# To create a protobuf file, we compile the input file
$(CWD)/%.pb.cc:		$(CWD)/%.proto
	$(HOSTBIN)/protoc $< -Imldb/ext/tensorflow --cpp_out=$(TF_CWD)

# We need to set up include paths and turn off a bunch of warnings.  Most of them
# are triggered by the (3rd party) Eigen library.
$(eval $(call set_compile_option,$(TENSORFLOW_CC_BUILD) $(TENSORFLOW_PROTOBUF_BUILD),-I$(CWD) -I$(INC) -I$(CWD)/third_party/eigen3 -Imldb/ext/re2 -Wno-reorder -Wno-return-type -Wno-overflow -Wno-overloaded-virtual -Wno-parentheses -Wno-maybe-uninitialized -Wno-array-bounds))

TENSORFLOW_LINK := protobuf re2 png

# Finally, build a library with the tensorflow functionality inside
$(eval $(call library,tensorflow,$(TENSORFLOW_CC_BUILD) $(TENSORFLOW_PROTOBUF_BUILD),$(TENSORFLOW_LINK)))

# And create a nice target name
tensorflow_lib: $(LIB)/libtensorflow.so

endif
