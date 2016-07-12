# External programs and libraries used by MLDB

EXT_SUBDIRS ?= tinyxml2 googleurl cityhash lz4 lzma sqlite judy svm libstemmer siphash svdlibc jsoncpp xxhash

$(eval $(call include_sub_makes,$(EXT_SUBDIRS)))

$(eval $(call include_sub_make,hoedown,hoedown,../hoedown.mk))
$(eval $(call include_sub_make,re2,re2,../re2.mk))
$(eval $(call include_sub_make,jpeg,jpeg,../jpeg.mk))
$(eval $(call include_sub_make,tensorflow,tensorflow,../tensorflow.mk))

$(eval $(call library,edlib,edlib/src/edlib.cpp))
$(eval $(call library,uap,uap-cpp/UaParser.cpp,yaml-cpp))
$(eval $(call include_sub_make,s2,s2-geometry-library/geometry,../../s2.mk))
