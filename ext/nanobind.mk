# Build instructions for nanobind, the Python binding library

NANOBIND_SOURCES := \
	nb_combined.cpp \

NANOBIND_LINK :=	m

NANOBIND_COMPILE_OPTIONS := \
	-fvisibility=hidden \
	-I$(EXT_DIR)/nanobind/include \
	-fPIC \
	-I $(EXT_DIR)/robin_map/include \
	-fno-strict-aliasing \
    -ffunction-sections \
	-fdata-sections \

#	-DNDEBUG \
#	-DNB_COMPACT_ASSERTIONS \


NANOBIND_LINK_OPTIONS := -L$(EXT_DIR)/nanobind -lnanobind

$(eval $(call library,nanobind,$(NANOBIND_SOURCES),$(NANOBIND_LINK)))
$(eval $(call set_compile_option,$(NANOBIND_SOURCES),$(NANOBIND_COMPILE_OPTIONS)))
