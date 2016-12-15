# Tensorflow needs version 3 of protobuf, which it bundles via a submodule.
# We start by building that.  Note that we need to compile the binaries to
# run on the HOST, as they are a build dependency, not on the target.
#
# We use a recursive make, since we don't care about anything apart from the
# final executable, the libraries, and the headers.

# NOTE TO USERS OF PROTOBUF
# This makefile defines variables that need to be used in order to
# properly use protobuf in other parts of the system:
#
# - To link with the protobuf library, you simply need to add protobuf3
#   to the list of libraries.  This makefile defines the LIB_protobuf3_DEPS
#   variable which ensures that the library will be built before anything
#   that links to it.  Anything that needs to explicitly depend on the
#   library needs to use the variable $(DEPENDS_ON_PROTOBUF_LIB) in the
#   dependencies.
# - For any source files that include protobuf headers, they need to
#   depend on $(DEPENDS_ON_PROTOBUF_INCLUDES) and they should include
#   -I$(PROTOBUF_INCLUDE_DIR) in the make flags.
# - Any command that needs to run the protobuf compiler needs to include
#   $(DEPENDS_ON_PROTOC) in the dependencies of the command, and to use
#   $(PROTOC) as the executable
#
# NO OTHER DEPENDENCIES ON PROTOBUF THAN THESE SHOULD BE USED.  If this
# rule is not followed, cross builds or docker builds will stop working.

PROTOBUF_INCLUDE_DIR:=$(PWD)/$(BUILD)/$(HOSTARCH)/include
PROTOC:=$(BUILD)/$(HOSTARCH)/bin/protoc
DEPENDS_ON_PROTOC:=$(PROTOC)
DEPENDS_ON_PROTOBUF_INCLUDES:=$(DEPENDS_ON_PROTOC)
DEPENDS_ON_PROTOBUF_LIB:=$(LIB)/libprotobuf3.so
LIB_protobuf3_DEPS:=$(DEPENDS_ON_PROTOBUF_LIB)


ifneq ($(PREMAKE),1)

# Macro to build protobuf for a given arch (either host or cross)
#
# $(1) = arch to build for (either $HOSTARCH or $ARCH)
# $(2) = place to install libraries

define build_protobuf_for_arch

# sne is string not equal, defined in gmsl
PROTOC_EXTRA_ARGS_$(1):=$(if $(call sne,$(1),$(HOSTARCH)),--with-protoc=$(PWD)/$(HOSTBIN)/protoc --host=$(1)-linux-gnu --target=$(1)-linux-gnu)$(if $(call seq,$(1),$(ARCH)), CC="$(CC)" CXX="$(CXX)")

# Find the absolute path of $(TMP), by adding $(PWD) if it's a relative
# path, in other words if it doesn't start with a /
TMP_ABSOLUTE_PATH:=$(if $(findstring xxxx/,xxxx$(TMP)),,$(PWD)/)$(TMP)

$(BUILD)/$(1)/bin/protoc: $(if $(call sne,$(1),$(HOSTARCH)),$(BUILD)/$(HOSTARCH)/bin/protoc)
	@mkdir -p $(TMP)
	@echo "   $(COLOR_BLUE)[COPY EXTERN]$(COLOR_RESET)                      	protobuf3 $(1)"
	@mkdir -p $(BUILD)/$(1)/tmp/protobuf-build
	@cp -rf mldb/ext/protobuf/* $(BUILD)/$(1)/tmp/protobuf-build
	@cp -rf $(PWD)/mldb/ext/gmock $(BUILD)/$(1)/tmp/protobuf-build/
	@echo " $(COLOR_BLUE)[CONFIG EXTERN]$(COLOR_RESET)                      	protobuf3 $(1)"
	@(cd $(BUILD)/$(1)/tmp/protobuf-build \
	&& ./autogen.sh > configure-log.txt 2>&1 \
	&& TMP=$(TMP_ABSOLUTE_PATH) ./configure \
		--prefix $(PWD)/$(BUILD)/$(1) \
		--program-suffix="" \
		$$(PROTOC_EXTRA_ARGS_$(1)) >> configure-log.txt 2>&1) \
	|| (echo $(COLOR_RED)Protobuf configure failed for $(1)$(COLOR_RESET) && cat $(BUILD)/$(1)/tmp/protobuf-build/configure-log.txt && false)
	@echo "   $(COLOR_BLUE)[MAKE EXTERN]$(COLOR_RESET)                      	protobuf3 $(1)"
	@+$(MAKE) -j -C $(BUILD)/$(1)/tmp/protobuf-build > $(BUILD)/$(1)/tmp/protobuf-build/make-log.txt 2>&1 || (echo $(COLOR_RED)Protobuf compile failed for $(1)$(COLOR_RESET) && cat $(BUILD)/$(1)/tmp/protobuf-build/make-log.txt && false)
	@echo "$(COLOR_BLUE)[INSTALL EXTERN]$(COLOR_RESET)                      	protobuf3 $(1)"
	@$(MAKE) -j install -C $(BUILD)/$(1)/tmp/protobuf-build > $(BUILD)/$(1)/tmp/protobuf-build/install-log.txt 2>&1 || (echo $(COLOR_RED)Protobuf install failed for $(1)$(COLOR_RESET) && cat $(BUILD)/$(1)/tmp/protobuf-build/install-log.txt && false)
	@echo "   $(COLOR_BLUE)[DONE EXTERN]$(COLOR_RESET)                      	protobuf3 $(1)"

# we call it libprotobuf3.so to avoid clashing with the system one
$(2)/libprotobuf3.so:	$(BUILD)/$(1)/bin/protoc | $(2)/.dir_exists
	@$(if $(call sne,$(2),$(BUILD)/$(1)/lib),cp $(PWD)/$(BUILD)/$(1)/lib/libprotobuf* $(2))
	@cp $(BUILD)/$(1)/lib/libprotobuf.so.10.0.0 $$@~ && mv $$@~ $$@

protobuf: $(BUILD)/$(1)/bin/protoc

endef

$(eval $(call build_protobuf_for_arch,$(ARCH),$(LIB)))
ifneq ($(ARCH),$(HOSTARCH))
$(eval $(call build_protobuf_for_arch,$(HOSTARCH),$(BUILD)/$(HOSTARCH)/lib))
endif
PROTOBUF_INCLUDE_DIR:=$(PWD)/$(BUILD)/$(ARCH)/include
PROTOC:=$(BUILD)/$(HOSTARCH)/bin/protoc
DEPENDS_ON_PROTOC:=$(PROTOC)
DEPENDS_ON_PROTOBUF_INCLUDES:=$(BUILD)/$(ARCH)/bin/protoc
DEPENDS_ON_PROTOBUF_LIB:=$(LIB)/libprotobuf3.so
LIB_protobuf3_DEPS:=$(DEPENDS_ON_PROTOBUF_LIB)

build_tools:   $(PROTOC)

endif
