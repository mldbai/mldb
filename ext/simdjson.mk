
SIMDJSON_SOURCE:= \
	src/simdjson.cpp \

SIMDJSON_INCLUDE_PATHS:=mldb/ext/simdjson/include

SIMDJSON_GCC_FLAGS:=-Wno-maybe-uninitialized -Wno-array-bounds -Wno-format-overflow -Wno-stringop-truncation -Wno-stringop-overflow
SIMDJSON_CLANG_FLAGS:=-Wno-maybe-uninitialized -Wno-format-overflow -Wno-stringop-truncation -Wno-stringop-overflow -Wno-unknown-warning-option

SIMDJSON_FLAGS= \
	$(if $(findstring gcc,$(toolchain)),$(SIMDJSON_GCC_FLAGS)) \
	$(if $(findstring clang,$(toolchain)),$(SIMDJSON_CLANG_FLAGS)) \
	$(foreach path,$(SIMDJSON_INCLUDE_PATHS), -I $(path))


SIMDJSON_DEFINES_Linux_x86_64:=
SIMDJSON_LIBS_Linux_x86_64:=

SIMDJSON_DEFINES_Linux_aarch64:=
SIMDJSON_LIBS_Linux_aarch64:=

SIMDJSON_DEFINES_Darwin_x86_64:=
SIMDJSON_LIBS_Darwin_x86_64:=

SIMDJSON_DEFINES_Darwin_arm64:=
SIMDJSON_LIBS_Darwin_arm64:=

SIMDJSON_DEFINES:=$(SIMDJSON_DEFINES_$(OSNAME)_$(ARCH))
SIMDJSON_LIBS:=$(SIMDJSON_LIBS_$(OSNAME)_$(ARCH))
#$(if $(SIMDJSON_DEFINES),,$(error SIMDJSON_DEFINES_$(OSNAME)_$(ARCH) not defined (unknown arch $(ARCH)).  Please define in libsimdjson.mk))

$(eval $(call set_compile_option,$(SIMDJSON_SOURCE),-Imldb/ext/simdjson/inclue -Imldb/ext/simdjson/src $(SIMDJSON_DEFINES) $(SIMDJSON_FLAGS)))

SIMDJSON_LIB_NAME:=simdjson

$(eval $(call library,$(SIMDJSON_LIB_NAME),$(SIMDJSON_SOURCE),$(SIMDJSON_LIBS)))

SIMDJSON_INCLUDE_DIR:=$(PWD)/libsimdjson
DEPENDS_ON_SIMDJSON_INCLUDES:=$(LIB)/$(SIMDJSON_LIB_NAME)$(SO_EXTENSION)
DEPENDS_ON_SIMDJSON_LIB:=$(LIB)/$(SIMDJSON_LIB_NAME)$(SO_EXTENSION)

libsimdjson:	$(DEPENDS_ON_SIMDJSON_LIB) $(DEPENDS_ON_SIMDJSON_INCLUDE)
