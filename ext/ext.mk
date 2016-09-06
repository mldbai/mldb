# External programs and libraries used by MLDB

EXT_SUBDIRS ?= tinyxml2 googleurl cityhash lz4 lzma sqlite judy svm libstemmer svdlibc jsoncpp xxhash

$(eval $(shell cp ext/protobuf_autogen.sh ext/protobuf/autogen.sh))

$(eval $(call include_sub_makes,$(EXT_SUBDIRS)))

$(eval $(call include_sub_make,hoedown,hoedown,../hoedown.mk))
$(eval $(call include_sub_make,re2,re2,../re2.mk))
$(eval $(call include_sub_make,jpeg,jpeg,../jpeg.mk))
$(eval $(call include_sub_make,protobuf,protobuf,../protobuf.mk))
$(eval $(call include_sub_make,farmhash,farmhash,../farmhash.mk))
$(eval $(call include_sub_make,highwayhash,highwayhash,../highwayhash.mk))
$(eval $(call include_sub_make,tensorflow,tensorflow,../tensorflow.mk))

EDLIB_CC_FILES:= edlib/edlib/src/edlib.cpp
EDLIB_WARNING_OPTIONS:=-Wno-char-subscripts
$(eval $(call set_compile_option,$(EDLIB_CC_FILES),$(EDLIB_WARNING_OPTIONS)))
$(eval $(call library,edlib,$(EDLIB_CC_FILES)))

$(eval $(call library,uap,uap-cpp/UaParser.cpp,yaml-cpp))
$(eval $(call include_sub_make,s2,s2-geometry-library/geometry,../../s2.mk))
