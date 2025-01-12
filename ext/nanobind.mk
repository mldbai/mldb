# Build instructions for nanobind, the Python binding library

NANOBIND_SOURCES := \
	src/nb_combined.cpp \

NANOBIND_LINK := \
	$(PYTHON_LIBRARY) \

NANOBIND_INCLUDE_PATH := mldb/ext/nanobind/include

NANOBIND_COMPILE_OPTIONS := \
	-Imldb/ext/nanobind/include \
	-Imldb/ext/robin-map/include \
	-I$(PYTHON_INCLUDE_PATH) \
	-fno-strict-aliasing \
    -ffunction-sections \
	-fdata-sections \
	-DNB_SHARED \

#	-fvisibility=hidden \
#	-DNDEBUG \
#	-DNB_COMPACT_ASSERTIONS \


NANOBIND_LINK_OPTIONS := -L$(EXT_DIR)/nanobind -lnanobind

$(eval $(call library,nanobind,$(NANOBIND_SOURCES),$(NANOBIND_LINK)))
$(eval $(call set_compile_option,$(NANOBIND_SOURCES),$(NANOBIND_COMPILE_OPTIONS)))
