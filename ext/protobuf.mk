# Tensorflow needs version 3 of protobuf, which it bundles via a submodule.
# We start by building that.  Note that we need to compile the binaries to
# run on the HOST, as they are a build dependency, not on the target.
#
# We use a recursive make, since we don't care about anything apart from the
# final executable.

ifneq ($(PREMAKE),1)

$(HOSTBIN)/protoc:
	@(cd mldb/ext/protobuf && ./autogen.sh && ./configure --prefix $(PWD)/$(BUILD)/$(HOSTARCH) && $(MAKE) -j && $(MAKE) -j install) > $(TMP)/protoc-build.log || (echo "protobuf build failed" && cat $(TMP)/protoc-build.log && false)

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

protobuf: $(HOSTBIN)/protoc $(INC)/google/protobuf $(LIB)/libprotobuf3.so

endif
