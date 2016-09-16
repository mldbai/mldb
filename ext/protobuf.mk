# Tensorflow needs version 3 of protobuf, which it bundles via a submodule.
# We start by building that.  Note that we need to compile the binaries to
# run on the HOST, as they are a build dependency, not on the target.
#
# We use a recursive make, since we don't care about anything apart from the
# final executable, the libraries, and the headers.

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
	@mkdir -p $(TMP)
	@echo "   $(COLOR_BLUE)[COPY EXTERN]$(COLOR_RESET)                      	protobuf3 $(1)"
	@mkdir -p $(BUILD)/$(1)/tmp/protobuf-build
	@cp -rf mldb/ext/protobuf/* $(BUILD)/$(1)/tmp/protobuf-build
	@cp -rf $(PWD)/mldb/ext/gmock $(BUILD)/$(1)/tmp/protobuf-build/
	@echo " $(COLOR_BLUE)[CONFIG EXTERN]$(COLOR_RESET)                      	protobuf3 $(1)"
	@(cd $(BUILD)/$(1)/tmp/protobuf-build \
	&& ./autogen.sh > configure-log.txt 2>&1 \
	&& TMP=$(PWD)/$(TMP) ./configure \
		--prefix $(PWD)/$(BUILD)/$(1) \
		--program-suffix="" \
		$$(PROTOC_EXTRA_ARGS_$(1)) >> configure-log.txt 2>&1) \
	|| (echo $(COLOR_RED)Protobuf configure failed for $(1)$(COLOR_RESET) && cat $(BUILD)/$(1)/tmp/protobuf-build/configure-log.txt && false)
	@echo "   $(COLOR_BLUE)[MAKE EXTERN]$(COLOR_RESET)                      	protobuf3 $(1)"
	@+$(MAKE) -j -C $(BUILD)/$(1)/tmp/protobuf-build > $(BUILD)/$(1)/tmp/protobuf-build/make-log.txt 2>&1 || (echo $(COLOR_RED)Protobuf compile failed for $(1)$(COLOR_RESET) && cat $(BUILD)/$(1)/tmp/protobuf-build/compile-log.txt && false)
	@echo "$(COLOR_BLUE)[INSTALL EXTERN]$(COLOR_RESET)                      	protobuf3 $(1)"
	@$(MAKE) -j install -C $(BUILD)/$(1)/tmp/protobuf-build > $(BUILD)/$(1)/tmp/protobuf-build/install-log.txt 2>&1 || (echo $(COLOR_RED)Protobuf install failed for $(1)$(COLOR_RESET) && cat $(BUILD)/$(1)/tmp/protobuf-build/install-log.txt && false)
	@echo "   $(COLOR_BLUE)[DONE EXTERN]$(COLOR_RESET)                      	protobuf3 $(1)"

# we call it libprotobuf3.so to avoid clashing with the system one
$(2)/libprotobuf3.so:	$(4)/protoc
	$(if $(call sne,$(2),$(BUILD)/$(1)/lib),cp $(PWD)/$(BUILD)/$(1)/lib/libprotobuf* $(2))
	@cp $(BUILD)/$(1)/lib/libprotobuf.so.10.0.0 $$@~ && mv $$@~ $$@

protobuf: $(4)/protoc

# Allow a dependency on the headers
$(INC)/google/protobuf: | $(4)/protoc
	@# Only copy when needing includes elsewhere
	[ ! "$(PWD)/$(BUILD)/$(1)/include/google/protobuf" -ef "$(3)/google/protobuf" ] \
	  && ( mkdir -p $(3)/google \
	     && cp -r $(PWD)/$(BUILD)/$(1)/include/google/protobuf $(3)/google/protobuf) \
	  || true

endef

ifneq ($(ARCH),$(HOSTARCH))
$(eval $(call build_protobuf_for_arch,$(HOSTARCH),$(BUILD)/$(HOSTARCH)/lib,$(BUILD)/$(HOSTARCH)/include,$(BUILD)/$(HOSTARCH)/bin))
endif
$(eval $(call build_protobuf_for_arch,$(ARCH),$(LIB),$(INC),$(BIN)))
endif
