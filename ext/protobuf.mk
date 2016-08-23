# Tensorflow needs version 3 of protobuf, which it bundles via a submodule.
# We start by building that.  Note that we need to compile the binaries to
# run on the HOST, as they are a build dependency, not on the target.
#
# We use a recursive make, since we don't care about anything apart from the
# final executable.

ifneq ($(PREMAKE),1)

# Macro to build protobuf for a given arch (either host or cross)
#
# $(1) = arch to build for (either $HOSTARCH or $ARCH)
# $(2) = place to install libraries
# $(3) = place to install headers
# $(4) = place to install binaries

define build_protobuf_for_arch

# sne is string not equal, defined in gmsl
PROTOC_EXTRA_ARGS_$(1):=$(if $(call sne,$(1),$(HOSTARCH)),--with-protoc=$(PWD)/$(HOSTBIN)/protoc --host=$(1)-linux-gnu --target=$(1)-linux-gnu)$(if $(call seq,$(1),$(ARCH)), CC="$(CC)" CXX="$(CXX)")

$(4)/protoc: $(if $(call sne,$(1),$(HOSTARCH)),$(HOSTBIN)/protoc)
	mkdir -p $(BUILD)/$(1)/tmp/protobuf-build
	cp -rf mldb/ext/protobuf/* $(BUILD)/$(1)/tmp/protobuf-build
	cd $(BUILD)/$(1)/tmp/protobuf-build \
	&& ./autogen.sh \
	&& ./configure \
		--prefix $(PWD)/$(BUILD)/$(1) \
		--program-suffix="" \
		$$(PROTOC_EXTRA_ARGS_$(1))
	+$(MAKE) -j -C $(BUILD)/$(1)/tmp/protobuf-build
	$(MAKE) -j install -C $(BUILD)/$(1)/tmp/protobuf-build

$(2)/libprotobuf3.so:	$(4)/protoc
	$(if $(call sne,$(2),$(BUILD)/$(1)/lib),cp $(PWD)/$(BUILD)/$(1)/lib/libprotobuf* $(2))
	cp $(BUILD)/$(1)/lib/libprotobuf.so.10.0.0 $$@~ && mv $$@~ $$@

#		--libdir=$(PWD)/$(2) \
#		--includedir=$(PWD)/$(3) \
#		--bindir=$(PWD)/$(4) \

# We use the protobuf library at runtime, so it needs to be installed in
# the right place.  Here we also rename it to avoid clashes with the system
# version.
#$(2)/libprotobuf3.so:	$(HOSTBIN)/protoc
#	@cp $(HOSTLIB)/libprotobuf.so $@

# We also need to make sure that the header files go in the right place
#$(INC)/google/protobuf: $(HOSTBIN)/protoc

protobuf: $(4)/protoc

endef

ifneq ($(ARCH),$(HOSTARCH))
$(eval $(call build_protobuf_for_arch,$(HOSTARCH),$(BUILD)/$(HOSTARCH)/lib,$(BUILD)/$(HOSTARCH)/include,$(BUILD)/$(HOSTARCH)/bin))
endif
$(eval $(call build_protobuf_for_arch,$(ARCH),$(LIB),$(INC),$(BIN)))

#else
#$(LIB)/libprotobuf3.so:	$(HOSTBIN)/protoc#
#	@cp $(HOSTLIB)/libprotobuf.so $@
#	@cp $(HOSTLIB)/libprotobuf.so.* $(LIB)

#$(INC)/google/protobuf:	$(HOSTBIN)/protoc
#	@mkdir -p $(INC)/google && ln -sf $(PWD)/$(HOSTINC)/google/protobuf $@
#endif

#protobuf: $(HOSTBIN)/protoc-$(ARCH) $(INC)/google/protobuf $(LIB)/libprotobuf3.so

endif
