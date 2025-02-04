# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBBLOCK_SOURCES:= \
	memory_region.cc \
	zip_serializer.cc \
	file_serializer.cc \
	content_descriptor.cc \
	content.cc \

$(eval $(call library,block,$(LIBBLOCK_SOURCES),vfs $(LIBARCHIVE_LIB_NAME) types arch db base value_description any watch))

$(eval $(call include_sub_make,testing))

