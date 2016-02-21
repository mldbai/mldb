# External programs and libraries used by MLDB

EXT_SUBDIRS ?= tinyxml2 googleurl cityhash lz4 lzma sqlite judy svm libstemmer siphash svdlibc jsoncpp xxhash libtwitcurl

$(eval $(call include_sub_makes,$(EXT_SUBDIRS)))

$(eval $(call include_sub_make,hoedown,hoedown,../hoedown.mk))
$(eval $(call include_sub_make,re2,re2,../re2.mk))
$(eval $(call include_sub_make,jpeg,jpeg,../jpeg.mk))
$(eval $(call include_sub_make,tensorflow,tensorflow,../tensorflow.mk))
