# External programs and libraries used by MLDB

EXT_SUBDIRS ?= tinyxml2 googleurl cityhash xxhash lz4 lzma sqlite judy svm libstemmer svdlibc jsoncpp

$(eval $(call include_sub_makes,$(EXT_SUBDIRS)))

$(eval $(call include_sub_make,hoedown,hoedown,../hoedown.mk))
$(eval $(call include_sub_make,re2,re2,../re2.mk))
$(eval $(call include_sub_make,jpeg,jpeg,../jpeg.mk))
$(eval $(call include_sub_make,protobuf,protobuf,../protobuf.mk))
$(eval $(call include_sub_make,farmhash,farmhash,../farmhash.mk))
$(eval $(call include_sub_make,highwayhash,highwayhash,../highwayhash.mk))
$(eval $(call include_sub_make,giflib,giflib,../giflib.mk))
$(eval $(call include_sub_make,tensorflow,tensorflow,../tensorflow.mk))
$(eval $(call include_sub_make,edlib,edlib,../edlib.mk))
$(eval $(call include_sub_make,zstd,zstd,../zstd.mk))
$(eval $(call include_sub_make,fasttext,fasttext,../fasttext.mk))

$(eval $(call include_sub_make,pffft,pffft,../pffft.mk))
$(eval $(call test,pffft_vectorization_test,pffft,boost))

$(eval $(call library,uap,uap-cpp/UaParser.cpp,yaml-cpp))
$(eval $(call include_sub_make,s2,s2-geometry-library/geometry,../../s2.mk))

EASYEXIF_CC_FILES:= easyexif/exif.cpp
$(eval $(call set_compile_option,$(EASYEXIF_CC_FILES),$(EASYEXIF_WARNING_OPTIONS)))
$(eval $(call library,easyexif,$(EASYEXIF_CC_FILES)))
$(eval $(call include_sub_make,libgit2,libgit2,../libgit2.mk))
$(eval $(call include_sub_make,casablanca,casablanca,../casablanca.mk))
$(eval $(call include_sub_make,azure-storage-cpp,azure-storage-cpp,../azure-storage-cpp.mk))
$(eval $(call include_sub_make,cmake,cmake,../cmake.mk))
$(eval $(call include_sub_make,llvm,llvm,../llvm.mk))
