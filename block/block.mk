# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBBLOCK_SOURCES:= \
	memory_region.cc \
	zip_serializer.cc \
	file_serializer.cc \
	content.cc \
	content_descriptor.cc \

LIBBLOCK_LINK:= \
	vfs \
	$(LIBARCHIVE_LIB_NAME) \
	types \

$(eval $(call library,block,$(LIBBLOCK_SOURCES),$(LIBBLOCK_LINK)))

$(eval $(call include_sub_make,testing))

