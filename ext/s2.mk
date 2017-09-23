# S2 geometry library support for MLDB

S2_CC_FILES:= \
	s1angle.cc \
	s1interval.cc \
	s2cap.cc \
	s2.cc \
	s2cell.cc \
	s2cellid.cc \
	s2cellunion.cc \
	s2edgeindex.cc \
	s2edgeutil.cc \
	s2latlng.cc \
	s2latlngrect.cc \
	s2loop.cc \
	s2pointregion.cc \
	s2polygonbuilder.cc \
	s2polygon.cc \
	s2polyline.cc \
	s2r2rect.cc \
	s2region.cc \
	s2regioncoverer.cc \
	s2regionintersection.cc \
	s2regionunion.cc \
	base/int128.cc \
	base/logging.cc \
	base/stringprintf.cc \
	base/strtoint.cc \
	strings/ascii_ctype.cc \
	strings/split.cc \
	strings/stringprintf.cc \
	strings/strutil.cc \
	util/coding/coder.cc \
	util/coding/varint.cc \
	util/hash/hash.cc \
	util/math/exactfloat/exactfloat.cc \
	util/math/mathlimits.cc \
	util/math/mathutil.cc

ifeq ($(toolchain),gcc)
S2_WARNING_OPTIONS:=-Wno-format-contains-nul -Wno-parentheses
endif
ifeq ($(toolchain),gcc5)
S2_WARNING_OPTIONS:=-Wno-format-contains-nul -Wno-parentheses
endif
ifeq ($(toolchain),gcc6)
S2_WARNING_OPTIONS:=-Wno-format-contains-nul -Wno-parentheses -Wno-unused-local-typedefs
endif
ifeq ($(toolchain),clang)
S2_WARNING_OPTIONS:=-Wno-parentheses -Wno-absolute-value -Wno-unused-local-typedef -Wno-unused-const-variable -Wno-format -Wno-dynamic-class-memaccess
endif

S2_COMPILE_OPTIONS:=-Imldb/ext/s2-geometry-library/geometry -Imldb/ext/s2-geometry-library/geometry/s2 -DS2_USE_EXACTFLOAT -DHASH_NAMESPACE=std

$(eval $(call set_compile_option,$(S2_CC_FILES),$(S2_COMPILE_OPTIONS) $(S2_WARNING_OPTIONS)))

$(eval $(call library,s2,$(S2_CC_FILES)))
